package app.aaps.pump.omnipod.common.bledriver.comm.legacy.session

import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattService
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.content.Context
import app.aaps.core.interfaces.configuration.Config
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.pump.omnipod.common.bledriver.comm.Ids
import app.aaps.pump.omnipod.common.bledriver.comm.endecrypt.EnDecrypt
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.ConnectException
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.io.CharacteristicType
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.callbacks.BleCommCallbacks
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.io.CmdBleIO
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.io.DataBleIO
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.io.IncomingPackets
import app.aaps.pump.omnipod.common.bledriver.comm.message.MessageIO
import app.aaps.pump.omnipod.common.bledriver.comm.session.ConnectionWaitCondition
import app.aaps.pump.omnipod.common.bledriver.comm.session.Session
import app.aaps.pump.omnipod.common.bledriver.comm.session.SessionKeys
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.eq
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.util.UUID
import java.util.concurrent.CountDownLatch

@Suppress("DEPRECATION")
@Timeout(10)
class ServiceDiscoveryRecoveryTest {

    private val logger = mock<AAPSLogger>()
    private val device = mock<BluetoothDevice>()
    private val gatt = mock<BluetoothGatt>()
    private val retryGatt = mock<BluetoothGatt>()
    private val callbackHistory = mutableListOf<BluetoothGattCallback>()
    private val context = mock<Context>()
    private val bluetoothManager = mock<BluetoothManager>()
    private val service = mock<BluetoothGattService>()
    private val cmdCharacteristic = mock<BluetoothGattCharacteristic>()
    private val dataCharacteristic = mock<BluetoothGattCharacteristic>()
    private val cmdIO = mock<CmdBleIO>()
    private val dataIO = mock<DataBleIO>()
    private lateinit var connection: TestConnection
    private lateinit var callbacks: BluetoothGattCallback
    private val serviceUuid = UUID.fromString("1a7e4024-e3ed-4464-8b7e-751e03d0dc5f")

    @BeforeEach
    fun setUp() {
        whenever(context.getSystemService(Context.BLUETOOTH_SERVICE)).thenReturn(bluetoothManager)
        whenever(bluetoothManager.getConnectionState(device, BluetoothProfile.GATT)).thenReturn(BluetoothProfile.STATE_CONNECTED)
        connection = TestConnection()
        whenever(device.connectGatt(eq(context), eq(false), any(), eq(BluetoothDevice.TRANSPORT_LE))).thenAnswer {
            val currentGatt = if (callbackHistory.isEmpty()) gatt else retryGatt
            callbacks = it.getArgument(2)
            callbackHistory.add(callbacks)
            callbacks.onConnectionStateChange(currentGatt, BluetoothGatt.GATT_SUCCESS, BluetoothProfile.STATE_CONNECTED)
            currentGatt
        }
        for (currentGatt in listOf(gatt, retryGatt)) {
            whenever(currentGatt.discoverServices()).thenAnswer {
                callbacks.onServicesDiscovered(currentGatt, BluetoothGatt.GATT_SUCCESS)
                true
            }
            whenever(currentGatt.getService(serviceUuid)).thenReturn(service)
            whenever(currentGatt.services).thenReturn(emptyList())
        }
        whenever(service.getCharacteristic(CharacteristicType.CMD.uuid)).thenReturn(cmdCharacteristic)
        whenever(service.getCharacteristic(CharacteristicType.DATA.uuid)).thenReturn(dataCharacteristic)
    }

    @Test
    fun `missing service after successful callback reconnects once before any messages`() {
        whenever(gatt.getService(serviceUuid)).thenReturn(null)
        whenever(gatt.discoverServices()).thenAnswer {
            verify(cmdIO, never()).hello()
            verify(cmdIO, never()).readyToRead()
            callbacks.onServicesDiscovered(gatt, BluetoothGatt.GATT_SUCCESS)
            true
        }

        connection.connect(ConnectionWaitCondition(timeoutMs = 10_000))

        inOrder(device, gatt, retryGatt) {
            verify(device).connectGatt(eq(context), eq(false), any(), eq(BluetoothDevice.TRANSPORT_LE))
            verify(gatt).discoverServices()
            verify(gatt).close()
            verify(device).connectGatt(eq(context), eq(false), any(), eq(BluetoothDevice.TRANSPORT_LE))
            verify(retryGatt).discoverServices()
        }
        verify(gatt, never()).connect()
        verify(retryGatt, never()).close()
        verify(cmdIO, times(1)).hello()
        verify(cmdIO, times(1)).readyToRead()
        verify(dataIO, times(1)).readyToRead()
        assertNotNull(connection.msgIO)
    }

    @Test
    fun `persistent missing service fails after two discoveries and closes each GATT once`() {
        whenever(gatt.getService(serviceUuid)).thenReturn(null)
        whenever(retryGatt.getService(serviceUuid)).thenReturn(null)

        val error = assertThrows<ConnectException> { connection.connect(ConnectionWaitCondition(timeoutMs = 10_000)) }

        assertTrue(error.message.orEmpty().contains("after one recovery"))
        verify(gatt, times(1)).discoverServices()
        verify(retryGatt, times(1)).discoverServices()
        verify(retryGatt, times(1)).close()
        verify(gatt, times(1)).close()
        assertNoMessages()
        connection.disconnect(false)
        connection.disconnect(true)
        verify(gatt, times(1)).close()
        verify(retryGatt, times(1)).close()
    }

    @Test
    fun `normal discovery uses one discovery without recovery`() {
        connection.connect(ConnectionWaitCondition(timeoutMs = 10_000))

        verify(gatt, times(1)).discoverServices()
        verify(gatt, times(1)).getService(serviceUuid)
        verify(gatt, never()).close()
        verify(retryGatt, never()).discoverServices()
        verify(device, times(1)).connectGatt(eq(context), eq(false), any(), eq(BluetoothDevice.TRANSPORT_LE))
        verify(cmdIO, times(1)).hello()
        verify(cmdIO, times(1)).readyToRead()
        verify(dataIO, times(1)).readyToRead()
    }

    @Test
    fun `missing characteristic is not retried and closes GATT before messaging`() {
        whenever(service.getCharacteristic(CharacteristicType.DATA.uuid)).thenReturn(null)

        assertThrows<ConnectException> { connection.connect(ConnectionWaitCondition(timeoutMs = 10_000)) }

        verify(gatt, times(1)).discoverServices()
        verify(gatt, times(1)).close()
        assertNoMessages()
    }

    @Test
    fun `discovery start failure is not retried and closes GATT`() {
        whenever(gatt.discoverServices()).thenReturn(false)

        assertThrows<ConnectException> { connection.connect(ConnectionWaitCondition(timeoutMs = 10_000)) }

        verify(gatt, times(1)).discoverServices()
        verify(gatt, times(1)).close()
        assertNoMessages()
    }

    @Test
    fun `cancellation after callback prevents recovery and closes GATT`() {
        val stop = CountDownLatch(1)
        whenever(gatt.discoverServices()).thenAnswer {
            callbacks.onServicesDiscovered(gatt, BluetoothGatt.GATT_SUCCESS)
            stop.countDown()
            true
        }

        assertThrows<ConnectException> { connection.connect(ConnectionWaitCondition(stopConnection = stop)) }

        verify(gatt, times(1)).discoverServices()
        verify(gatt, times(1)).close()
        assertNoMessages()
    }

    @Test
    fun `cancellation during failed GATT cleanup prevents opening another GATT`() {
        val stop = CountDownLatch(1)
        whenever(gatt.getService(serviceUuid)).thenReturn(null)
        doAnswer {
            stop.countDown()
            null
        }.whenever(gatt).close()

        assertThrows<ConnectException> { connection.connect(ConnectionWaitCondition(stopConnection = stop)) }

        verify(gatt, times(1)).close()
        verify(retryGatt, never()).discoverServices()
        verify(device, times(1)).connectGatt(eq(context), eq(false), any(), eq(BluetoothDevice.TRANSPORT_LE))
        assertNoMessages()
    }

    @Test
    fun `interruption after callback closes GATT without starting recovery`() {
        whenever(gatt.discoverServices()).thenAnswer {
            callbacks.onServicesDiscovered(gatt, BluetoothGatt.GATT_SUCCESS)
            Thread.currentThread().interrupt()
            true
        }
        try {
            assertThrows<InterruptedException> { connection.connect(ConnectionWaitCondition(timeoutMs = 10_000)) }
        } finally {
            Thread.interrupted()
        }

        verify(gatt, times(1)).discoverServices()
        verify(gatt, times(1)).close()
        assertNoMessages()
    }

    @Test
    fun `discovery timeout does not consume service list or start recovery`() {
        whenever(gatt.discoverServices()).thenReturn(true)
        val bleCallbacks = BleCommCallbacks(logger, IncomingPackets(), connection)
        val discoverer = ServiceDiscoverer(logger, gatt, bleCallbacks, connection)

        val error = assertThrows<ConnectException> { discoverer.discoverServices(ConnectionWaitCondition(timeoutMs = 30)) }

        assertEquals("Service discovery timeout", error.message)
        verify(gatt, times(1)).discoverServices()
        verify(gatt, never()).getService(any())
    }

    @Test
    fun `recovery delay must fit the original connection budget`() {
        whenever(gatt.getService(serviceUuid)).thenReturn(null)

        val error = assertThrows<ConnectException> { connection.connect(ConnectionWaitCondition(timeoutMs = 100)) }

        assertEquals("connection timeout", error.message)
        verify(gatt, times(1)).discoverServices()
        verify(gatt, times(1)).close()
        verify(retryGatt, never()).discoverServices()
        assertNoMessages()
    }

    @Test
    fun `late disconnect and discovery callbacks from retired GATT cannot disturb retry`() {
        val stop = CountDownLatch(1)
        whenever(gatt.getService(serviceUuid)).thenReturn(null)
        whenever(retryGatt.discoverServices()).thenAnswer {
            val retiredCallbacks = callbackHistory.first()
            retiredCallbacks.onConnectionStateChange(gatt, BluetoothGatt.GATT_SUCCESS, BluetoothProfile.STATE_DISCONNECTED)
            retiredCallbacks.onServicesDiscovered(gatt, BluetoothGatt.GATT_SUCCESS)
            callbacks.onServicesDiscovered(retryGatt, BluetoothGatt.GATT_SUCCESS)
            true
        }

        connection.connect(ConnectionWaitCondition(stopConnection = stop))

        assertEquals(1L, stop.count)
        verify(gatt, times(1)).close()
        verify(retryGatt, never()).close()
        verify(cmdIO, times(1)).hello()
        assertNotNull(connection.msgIO)
    }

    @Test
    fun `fresh GATT must receive its own discovery callback within the shared deadline`() {
        whenever(gatt.getService(serviceUuid)).thenReturn(null)
        whenever(retryGatt.discoverServices()).thenReturn(true)

        assertThrows<ConnectException> { connection.connect(ConnectionWaitCondition(timeoutMs = 900)) }

        verify(gatt, times(1)).close()
        verify(retryGatt, times(1)).discoverServices()
        verify(retryGatt, never()).getService(any())
        verify(retryGatt, times(1)).close()
        assertNoMessages()
    }

    @Test
    fun `successful discovery from a different GATT does not release the waiter`() {
        val bleCallbacks = BleCommCallbacks(logger, IncomingPackets(), connection)
        bleCallbacks.startServiceDiscovery(gatt)

        bleCallbacks.onServicesDiscovered(mock(), BluetoothGatt.GATT_SUCCESS)
        assertFalse(bleCallbacks.waitForServiceDiscovery(0))

        bleCallbacks.onServicesDiscovered(gatt, BluetoothGatt.GATT_SUCCESS)
        assertTrue(bleCallbacks.waitForServiceDiscovery(0))
    }

    @Test
    fun `reset and failed status cannot masquerade as successful discovery`() {
        val bleCallbacks = BleCommCallbacks(logger, IncomingPackets(), connection)
        bleCallbacks.startServiceDiscovery(gatt)
        bleCallbacks.onServicesDiscovered(gatt, 133)
        assertFalse(bleCallbacks.waitForServiceDiscovery(0))

        bleCallbacks.resetConnection()
        assertFalse(bleCallbacks.waitForServiceDiscovery(0))
    }

    private fun assertNoMessages() {
        verify(cmdIO, never()).hello()
        verify(cmdIO, never()).readyToRead()
        verify(dataIO, never()).readyToRead()
        assertNull(connection.msgIO)
        assertNull(connection.session)
    }

    private inner class TestConnection : SharedBleConnection(device, logger, mock<Config>(), context) {

        override val connectionName = "Dash test"
        override fun recordConnectionAttempt() = Unit
        override fun updateConnectionState(state: LifecycleState) = Unit
        override fun connectGatt(): BluetoothGatt? = device.connectGatt(context, false, bleCommCallbacks, BluetoothDevice.TRANSPORT_LE)
        override fun hello(cmdBleIO: CmdBleIO) {
            cmdBleIO.hello()
        }

        override fun createMessageIO(gatt: BluetoothGatt, discovered: Map<CharacteristicType, BluetoothGattCharacteristic>): Pair<CmdBleIO, DataBleIO> {
            assertEquals(cmdCharacteristic, discovered.getValue(CharacteristicType.CMD))
            assertEquals(dataCharacteristic, discovered.getValue(CharacteristicType.DATA))
            return cmdIO to dataIO
        }

        override fun createSession(messageIO: MessageIO, ids: Ids, keys: SessionKeys, enDecrypt: EnDecrypt): Session = mock()
    }
}
