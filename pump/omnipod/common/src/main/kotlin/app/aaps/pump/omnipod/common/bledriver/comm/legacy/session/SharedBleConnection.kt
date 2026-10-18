package app.aaps.pump.omnipod.common.bledriver.comm.legacy.session

import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.content.Context
import app.aaps.core.data.configuration.Constants
import app.aaps.core.interfaces.configuration.Config
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.utils.toHex
import app.aaps.pump.omnipod.common.bledriver.comm.Ids
import app.aaps.pump.omnipod.common.bledriver.comm.endecrypt.EnDecrypt
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.ConnectException
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.FailedToConnectException
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.io.CharacteristicType
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.session.BleConnection
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.callbacks.BleCommCallbacks
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.io.CmdBleIO
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.io.DataBleIO
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.io.IncomingPackets
import app.aaps.pump.omnipod.common.bledriver.comm.message.MessageIO
import app.aaps.pump.omnipod.common.bledriver.comm.session.Connected
import app.aaps.pump.omnipod.common.bledriver.comm.session.ConnectionState
import app.aaps.pump.omnipod.common.bledriver.comm.session.ConnectionWaitCondition
import app.aaps.pump.omnipod.common.bledriver.comm.session.DisconnectHandler
import app.aaps.pump.omnipod.common.bledriver.comm.session.EapSqn
import app.aaps.pump.omnipod.common.bledriver.comm.session.NotConnected
import app.aaps.pump.omnipod.common.bledriver.comm.session.STOP_CONNECTING_CHECK_INTERVAL_MS
import app.aaps.pump.omnipod.common.bledriver.comm.session.Session
import app.aaps.pump.omnipod.common.bledriver.comm.session.SessionEstablisher
import app.aaps.pump.omnipod.common.bledriver.comm.session.SessionKeys
import app.aaps.pump.omnipod.common.bledriver.comm.session.SessionNegotiationResynchronization
import java.util.concurrent.TimeUnit

abstract class SharedBleConnection(
    protected val podDevice: BluetoothDevice,
    protected val aapsLogger: AAPSLogger,
    protected val config: Config,
    protected val context: Context
) : BleConnection, DisconnectHandler {

    protected abstract val connectionName: String
    protected val incomingPackets = IncomingPackets()
    @Volatile
    protected var bleCommCallbacks = createCallbacks()
        private set
    protected var gattConnection: BluetoothGatt? = null
    private val bluetoothManager = context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager?
    @Volatile private var connectionWaitCondition: ConnectionWaitCondition? = null

    @Volatile
    final override var session: Session? = null

    @Volatile
    final override var msgIO: MessageIO? = null

    protected abstract fun recordConnectionAttempt()
    protected abstract fun updateConnectionState(state: LifecycleState)
    protected abstract fun connectGatt(): BluetoothGatt?
    protected open fun prepareGatt(gatt: BluetoothGatt) = Unit
    protected abstract fun hello(cmdBleIO: CmdBleIO)
    protected abstract fun createSession(messageIO: MessageIO, ids: Ids, keys: SessionKeys, enDecrypt: EnDecrypt): Session
    protected open fun discoverServices(
        gatt: BluetoothGatt,
        connectionWaitCond: ConnectionWaitCondition,
        deadline: Long
    ): Map<CharacteristicType, BluetoothGattCharacteristic> =
        ServiceDiscoverer(aapsLogger, gatt, bleCommCallbacks, this).discoverServices(connectionWaitCond, deadline)

    protected open fun createMessageIO(
        gatt: BluetoothGatt,
        discovered: Map<CharacteristicType, BluetoothGattCharacteristic>
    ): Pair<CmdBleIO, DataBleIO> {
        val cmdBleIO = CmdBleIO(
            aapsLogger,
            discovered.getValue(CharacteristicType.CMD),
            incomingPackets.cmdQueue,
            gatt,
            bleCommCallbacks
        )
        val dataBleIO = DataBleIO(
            aapsLogger,
            discovered.getValue(CharacteristicType.DATA),
            incomingPackets.dataQueue,
            gatt,
            bleCommCallbacks
        )
        return cmdBleIO to dataBleIO
    }

    @Synchronized
    final override fun connect(connectionWaitCond: ConnectionWaitCondition) {
        aapsLogger.debug(LTag.PUMPBTCOMM, "Connecting $connectionName connectionWaitCond=$connectionWaitCond")
        connectionWaitCondition = connectionWaitCond
        recordConnectionAttempt()
        updateConnectionState(LifecycleState.CONNECTING)
        // One budget covers connect, both discoveries and the retry; there is no separate discovery floor.
        val timeoutMs = connectionWaitCond.timeoutMs ?: MAX_WAIT_FOR_CONNECTION_SECONDS * 1000L
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
        val (gatt, discovered) = try {
            connectAndDiscover(connectionWaitCond, deadline)
        } catch (ex: Exception) {
            connectionWaitCondition = null
            disconnect(true)
            throw ex
        }
        prepareGatt(gatt)
        val (cmdBleIO, dataBleIO) = createMessageIO(gatt, discovered)
        msgIO = MessageIO(aapsLogger, cmdBleIO, dataBleIO)
        hello(cmdBleIO)
        cmdBleIO.readyToRead()
        dataBleIO.readyToRead()
        connectionWaitCondition = null
    }

    private fun connectAndDiscover(
        connectionWaitCond: ConnectionWaitCondition,
        deadline: Long
    ): Pair<BluetoothGatt, Map<CharacteristicType, BluetoothGattCharacteristic>> {
        try {
            return connectAndDiscoverOnce(connectionWaitCond, deadline)
        } catch (_: MissingServiceException) {
            aapsLogger.warn(LTag.PUMPBTCOMM, "DASH service missing after successful discovery; reconnecting once with fresh GATT")
        }
        disconnect(true)
        checkConnectionWait(connectionWaitCond, deadline)
        if (remainingMs(deadline) <= MISSING_SERVICE_RETRY_DELAY_MS) throw ConnectException("connection timeout")
        Thread.sleep(MISSING_SERVICE_RETRY_DELAY_MS)
        updateConnectionState(LifecycleState.CONNECTING)
        try {
            return connectAndDiscoverOnce(connectionWaitCond, deadline)
                .also { aapsLogger.info(LTag.PUMPBTCOMM, "DASH service recovered on fresh GATT retry") }
        } catch (ex: MissingServiceException) {
            throw ConnectException("${ex.message} (after one recovery)")
        }
    }

    private fun connectAndDiscoverOnce(
        connectionWaitCond: ConnectionWaitCondition,
        deadline: Long
    ): Pair<BluetoothGatt, Map<CharacteristicType, BluetoothGattCharacteristic>> {
        checkConnectionWait(connectionWaitCond, deadline)
        var gatt = gattConnection
        if (gatt == null) {
            gatt = connectGatt()
            if (gatt == null) {
                Thread.sleep(minOf(SLEEP_WHEN_FAILING_TO_CONNECT_GATT, remainingMs(deadline).coerceAtLeast(0)))
                throw FailedToConnectException("connectGatt() returned null")
            }
            gattConnection = gatt
        } else if (!gatt.connect()) {
            throw FailedToConnectException("connect() returned false")
        }
        if (waitForConnection(remainingWaitCondition(connectionWaitCond, deadline), deadline) !is Connected) {
            throw FailedToConnectException(podDevice.address)
        }
        updateConnectionState(LifecycleState.CONNECTED)
        return gatt to discoverServices(gatt, remainingWaitCondition(connectionWaitCond, deadline), deadline)
    }

    private fun remainingWaitCondition(condition: ConnectionWaitCondition, deadline: Long): ConnectionWaitCondition {
        checkConnectionWait(condition, deadline)
        return if (condition.stopConnection != null) condition else ConnectionWaitCondition(timeoutMs = remainingMs(deadline))
    }

    private fun checkConnectionWait(condition: ConnectionWaitCondition, deadline: Long) {
        if (Thread.currentThread().isInterrupted) throw InterruptedException("Connection interrupted")
        if (condition.stopConnection?.count == 0L) throw ConnectException("stopConnecting called")
        if (remainingMs(deadline) <= 0) throw ConnectException("connection timeout")
    }

    private fun remainingMs(deadline: Long): Long = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())

    private fun createCallbacks(): BleCommCallbacks {
        lateinit var callbacks: BleCommCallbacks
        callbacks = BleCommCallbacks(aapsLogger, incomingPackets, object : DisconnectHandler {
            override fun onConnectionLost(status: Int) = this@SharedBleConnection.onConnectionLost(callbacks, status)
        })
        return callbacks
    }

    @Synchronized
    final override fun disconnect(closeGatt: Boolean) {
        aapsLogger.debug(LTag.PUMPBTCOMM, "Disconnecting $connectionName closeGatt=$closeGatt")
        if (!closeGatt && gattConnection != null) {
            gattConnection?.disconnect()
            updateConnectionState(LifecycleState.DISCONNECTED)
        } else {
            // Retire callbacks before closing: a late disconnect must not close the next GATT.
            val oldGatt = gattConnection
            val oldCallbacks = bleCommCallbacks
            gattConnection = null
            bleCommCallbacks = createCallbacks()
            oldCallbacks.resetConnection()
            session = null
            msgIO = null
            updateConnectionState(LifecycleState.DISCONNECTED)
            oldGatt?.close()
        }
    }

    private fun waitForConnection(connectionWaitCond: ConnectionWaitCondition, deadline: Long): ConnectionState {
        aapsLogger.debug(LTag.PUMPBTCOMM, "waitForConnection connectionWaitCond=$connectionWaitCond")
        try {
            connectionWaitCond.timeoutMs?.let(bleCommCallbacks::waitForConnection)
            connectionWaitCond.stopConnection?.let {
                while (!bleCommCallbacks.waitForConnection(minOf(remainingMs(deadline), STOP_CONNECTING_CHECK_INTERVAL_MS))) {
                    checkConnectionWait(connectionWaitCond, deadline)
                }
            }
        } catch (ex: InterruptedException) {
            Thread.currentThread().interrupt()
            throw ex
        }
        checkConnectionWait(connectionWaitCond, deadline)
        return connectionState()
    }

    final override fun connectionState(): ConnectionState {
        val connectionState = bluetoothManager?.getConnectionState(podDevice, BluetoothProfile.GATT)
        aapsLogger.debug(LTag.PUMPBTCOMM, "GATT connection state: $connectionState")
        return if (connectionState == BluetoothProfile.STATE_CONNECTED) {
            Connected
        } else {
            NotConnected
        }
    }

    final override fun establishSession(ltk: ByteArray, msgSeq: Byte, ids: Ids, eapSqn: ByteArray): EapSqn? {
        val messageIO = msgIO ?: throw ConnectException("Connection lost")
        return when (val keys = SessionEstablisher(aapsLogger, config, messageIO, ltk, eapSqn, ids, msgSeq).negotiateSessionKeys()) {
            is SessionNegotiationResynchronization -> {
                if (config.DEBUG) aapsLogger.info(LTag.PUMPCOMM, "EAP AKA resynchronization: ${keys.synchronizedEapSqn}")
                keys.synchronizedEapSqn
            }

            is SessionKeys                         -> {
                if (config.DEBUG) {
                    aapsLogger.info(LTag.PUMPCOMM, "CK: ${keys.ck.toHex()}")
                    aapsLogger.info(LTag.PUMPCOMM, "msgSequenceNumber: ${keys.msgSequenceNumber}")
                    aapsLogger.info(LTag.PUMPCOMM, "Nonce: ${keys.nonce}")
                }
                val enDecrypt = EnDecrypt(aapsLogger, keys.nonce, keys.ck)
                session = createSession(messageIO, ids, keys, enDecrypt)
                null
            }
        }
    }

    final override fun onConnectionLost(status: Int) = onConnectionLost(bleCommCallbacks, status)

    private fun onConnectionLost(callbacks: BleCommCallbacks, status: Int) {
        if (callbacks !== bleCommCallbacks) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "Ignoring disconnect from retired GATT")
            return
        }
        aapsLogger.info(LTag.PUMPBTCOMM, "Lost $connectionName connection with status: $status")
        connectionWaitCondition?.stopConnection?.let {
            if (it.count > 0) it.countDown()
        }
        synchronized(this) {
            if (callbacks === bleCommCallbacks) disconnect(true)
        }
    }

    companion object {
        const val MAX_WAIT_FOR_CONNECTION_SECONDS = Constants.PUMP_MAX_CONNECTION_TIME_IN_SECONDS + 10
        const val SLEEP_WHEN_FAILING_TO_CONNECT_GATT = 10_000L
        private const val MISSING_SERVICE_RETRY_DELAY_MS = 500L
    }

    protected enum class LifecycleState {
        CONNECTING,
        CONNECTED,
        DISCONNECTED
    }
}
