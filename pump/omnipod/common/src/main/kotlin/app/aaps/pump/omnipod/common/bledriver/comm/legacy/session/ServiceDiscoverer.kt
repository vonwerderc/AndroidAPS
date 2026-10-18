package app.aaps.pump.omnipod.common.bledriver.comm.legacy.session

import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattService
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.ConnectException
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.io.CharacteristicType
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.session.BleConnection
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.callbacks.BleCommCallbacks
import app.aaps.pump.omnipod.common.bledriver.comm.session.Connected
import app.aaps.pump.omnipod.common.bledriver.comm.session.ConnectionWaitCondition
import app.aaps.pump.omnipod.common.bledriver.comm.session.STOP_CONNECTING_CHECK_INTERVAL_MS
import java.math.BigInteger
import java.util.UUID
import java.util.concurrent.TimeUnit

class ServiceDiscoverer(
    private val logger: AAPSLogger,
    private val gatt: BluetoothGatt,
    private val bleCallbacks: BleCommCallbacks,
    private val connection: BleConnection
) {

    /**
     * This is first step after connection establishment
     */
    fun discoverServices(connectionWaitCond: ConnectionWaitCondition, deadline: Long): Map<CharacteristicType, BluetoothGattCharacteristic> {
        val service = discoverService(connectionWaitCond, deadline)
            ?: throw MissingServiceException()
        val cmdChar = service.getCharacteristic(CharacteristicType.CMD.uuid)
            ?: throw ConnectException("Characteristic not found: ${CharacteristicType.CMD.value}")
        val dataChar = service.getCharacteristic(CharacteristicType.DATA.uuid)
            ?: throw ConnectException("Characteristic not found: ${CharacteristicType.DATA.value}")
        return mapOf(
            CharacteristicType.CMD to cmdChar,
            CharacteristicType.DATA to dataChar
        )
    }

    private fun discoverService(connectionWaitCond: ConnectionWaitCondition, deadline: Long): BluetoothGattService? {
        checkConnection(connectionWaitCond, deadline)
        logger.debug(LTag.PUMPBTCOMM, "Discovering services gatt=${System.identityHashCode(gatt)}")
        bleCallbacks.startServiceDiscovery()
        try {
            if (!gatt.discoverServices()) throw ConnectException("Could not start discovering services")
        } catch (_: SecurityException) {
            throw ConnectException("Missing bluetooth permission")
        }
        while (true) {
            checkConnection(connectionWaitCond, deadline)
            if (bleCallbacks.waitForServiceDiscovery(minOf(remainingMs(deadline), STOP_CONNECTING_CHECK_INTERVAL_MS))) break
        }
        checkConnection(connectionWaitCond, deadline)
        logger.debug(LTag.PUMPBTCOMM, "Services discovered")
        return gatt.getService(SERVICE_UUID.toUuid()) ?: run {
            logger.debug(LTag.PUMPBTCOMM, "DASH discovery service count: ${gatt.services.size}")
            for (svc in gatt.services) logger.debug(LTag.PUMPBTCOMM, "Found service: ${svc.uuid}")
            null
        }
    }

    private fun checkConnection(connectionWaitCond: ConnectionWaitCondition, deadline: Long) {
        if (Thread.currentThread().isInterrupted) throw InterruptedException("Service discovery interrupted")
        if (connectionWaitCond.stopConnection?.count == 0L) throw ConnectException("stopConnecting called")
        if (connection.connectionState() !is Connected) throw ConnectException("Connection lost during service discovery")
        if (remainingMs(deadline) <= 0) throw ConnectException("Service discovery timeout")
    }

    private fun remainingMs(deadline: Long): Long = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())

    private fun String.toUuid(): UUID = UUID(
        BigInteger(replace("-", "").substring(0, 16), 16).toLong(),
        BigInteger(replace("-", "").substring(16), 16).toLong()
    )

    companion object {
        internal const val SERVICE_UUID = "1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f"
    }
}

internal class MissingServiceException : Exception("Service not found: ${ServiceDiscoverer.SERVICE_UUID}")
