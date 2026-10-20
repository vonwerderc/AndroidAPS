package app.aaps.pump.omnipod.dash.driver

import app.aaps.pump.omnipod.common.bledriver.comm.Ids
import app.aaps.pump.omnipod.common.bledriver.comm.OmnipodDashBleManagerImpl
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.BusyException
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.ConnectException
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.CouldNotSendCommandException
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.FailedToConnectException
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.MessageIOException
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.NotConnectedException
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.SessionEstablishmentException
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.device.BleDeviceManager
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.scan.BleDiscoveredDevice
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.scan.PodScanner
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.session.BleConnection
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.session.BleConnectionFactory
import app.aaps.pump.omnipod.common.bledriver.comm.session.CommandAckError
import app.aaps.pump.omnipod.common.bledriver.comm.session.CommandReceiveError
import app.aaps.pump.omnipod.common.bledriver.comm.session.CommandReceiveSuccess
import app.aaps.pump.omnipod.common.bledriver.comm.session.CommandSendErrorConfirming
import app.aaps.pump.omnipod.common.bledriver.comm.session.CommandSendErrorSending
import app.aaps.pump.omnipod.common.bledriver.comm.session.CommandSendSuccess
import app.aaps.pump.omnipod.common.bledriver.comm.session.Connected
import app.aaps.pump.omnipod.common.bledriver.comm.session.ConnectionWaitCondition
import app.aaps.pump.omnipod.common.bledriver.comm.session.EapSqn
import app.aaps.pump.omnipod.common.bledriver.comm.session.NotConnected
import app.aaps.pump.omnipod.common.bledriver.comm.session.Session
import app.aaps.pump.omnipod.common.bledriver.event.PodEvent
import app.aaps.pump.omnipod.common.bledriver.pod.command.base.Command
import app.aaps.pump.omnipod.common.bledriver.pod.response.Response
import app.aaps.pump.omnipod.common.bledriver.pod.state.OmnipodDashPodStateManager
import com.google.common.truth.Truth.assertThat
import io.reactivex.rxjava3.observers.TestObserver
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.util.concurrent.CountDownLatch

/**
 * Tests the concrete DASH manager through its public API, with mocked BLE and session boundaries.
 * These tests cover DASH events and state updates, not GATT callbacks or the pairing protocol.
 */
class OmnipodDashBleManagerTest {

    private val podState: OmnipodDashPodStateManager = mock()
    private val deviceManager: BleDeviceManager = mock()
    private val connectionFactory: BleConnectionFactory = mock()
    private val connection: BleConnection = mock()
    private val session: Session = mock()
    private val command: Command = mock()
    private val response: Response = mock()
    private val address = "00:11:22:33:44:55"
    private val ltk = ByteArray(16) { it.toByte() }
    private val firstSqn = EapSqn(1).value
    private lateinit var manager: OmnipodDashBleManagerImpl

    @BeforeEach
    fun setUp() {
        whenever(podState.uniqueId).thenReturn(123456L)
        whenever(podState.bluetoothAddress).thenReturn(address)
        whenever(podState.ltk).thenReturn(ltk)
        whenever(podState.successfulConnections).thenReturn(3)
        whenever(podState.increaseEapAkaSequenceNumber()).thenReturn(firstSqn)
        whenever(deviceManager.isBluetoothAvailable()).thenReturn(true)
        whenever(deviceManager.ensureBondedIfRequired(address)).thenReturn(true)
        whenever(connectionFactory.createConnection(address)).thenReturn(connection)
        whenever(connection.connectionState()).thenReturn(NotConnected)
        whenever(connection.session).thenReturn(session)
        whenever(session.sendCommand(command)).thenReturn(CommandSendSuccess)
        whenever(session.readAndAckResponse()).thenReturn(CommandReceiveSuccess(response))
        manager = OmnipodDashBleManagerImpl(mock(), podState, mock(), connectionFactory, deviceManager)
    }

    @Test
    fun `connect emits DASH connection events and commits the sequence number`() {
        assertConnected(manager.connect(12_345L).test())

        val ids = argumentCaptor<Ids>()
        inOrder(connection, podState) {
            verify(connection).connect(ConnectionWaitCondition(timeoutMs = 12_345L))
            verify(podState).increaseEapAkaSequenceNumber()
            verify(connection).establishSession(eq(ltk), eq(1.toByte()), ids.capture(), eq(firstSqn))
            verify(podState).successfulConnections = 4
            verify(podState).commitEapAkaSequenceNumber()
        }
        assertThat(ids.firstValue.myId.toLong()).isEqualTo(OmnipodDashBleManagerImpl.CONTROLLER_ID.toLong())
        assertThat(ids.firstValue.podId.toLong()).isEqualTo(123456L)
        verify(connectionFactory).createConnection(address)
        verify(deviceManager).ensureBondedIfRequired(address)
    }

    @Test
    fun `connect passes the stop latch to the connection`() {
        val latch = CountDownLatch(1)

        assertConnected(manager.connect(latch).test())

        verify(connection).connect(ConnectionWaitCondition(stopConnection = latch))
    }

    @Test
    fun `an existing session is reused without changing DASH connection counters`() {
        connectForCommand()
        whenever(connection.connectionState()).thenReturn(Connected)

        manager.connect(1000).test()
            .assertComplete()
            .assertNoErrors()
            .assertValueCount(2)
            .assertValueAt(0, PodEvent.BluetoothConnecting)
            .assertValueAt(1) { it is PodEvent.AlreadyConnected && it.bluetoothAddress == address }

        verify(connection, never()).connect(any())
        verify(connection, never()).establishSession(any(), any(), any(), any())
        verify(podState, never()).increaseEapAkaSequenceNumber()
        verify(podState, never()).successfulConnections = any()
        verify(podState, never()).commitEapAkaSequenceNumber()
        assertCommandSuccess()
    }

    @Test
    fun `a connected pod without a session establishes a new session`() {
        whenever(connection.connectionState()).thenReturn(Connected)
        whenever(connection.session).thenReturn(null)

        assertConnected(manager.connect(1000).test())

        verify(connection).connect(any())
        verify(connection).establishSession(eq(ltk), eq(1.toByte()), any(), eq(firstSqn))
    }

    @Test
    fun `one resynchronization updates DASH state and retries with the next sequence number`() {
        val nextSqn = EapSqn(101).value
        whenever(podState.increaseEapAkaSequenceNumber()).thenReturn(firstSqn, nextSqn)
        whenever(connection.establishSession(any(), any(), any(), any())).thenReturn(EapSqn(100), null)

        assertConnected(manager.connect(1000).test())

        inOrder(connection, podState) {
            verify(podState).increaseEapAkaSequenceNumber()
            verify(connection).establishSession(eq(ltk), eq(1.toByte()), any(), eq(firstSqn))
            verify(podState).eapAkaSequenceNumber = 100L
            verify(podState).increaseEapAkaSequenceNumber()
            verify(connection).establishSession(eq(ltk), eq(1.toByte()), any(), eq(nextSqn))
            verify(podState).successfulConnections = 4
            verify(podState).commitEapAkaSequenceNumber()
        }
    }

    @Test
    fun `a second resynchronization fails without recording a successful connection`() {
        whenever(connection.establishSession(any(), any(), any(), any())).thenReturn(EapSqn(100), EapSqn(200))

        manager.connect(1000).test()
            .assertError(SessionEstablishmentException::class.java)
            .assertNotComplete()
            .assertValueCount(3)
            .assertValueAt(0, PodEvent.BluetoothConnecting)
            .assertValueAt(1) { it is PodEvent.BluetoothConnected && it.bluetoothAddress == address }
            .assertValueAt(2, PodEvent.EstablishingSession)

        verify(connection, times(2)).establishSession(any(), any(), any(), any())
        verify(connection).disconnect(false)
        verify(podState, never()).successfulConnections = any()
        verify(podState, never()).commitEapAkaSequenceNumber()
        whenever(connection.establishSession(any(), any(), any(), any())).thenReturn(null)
        assertConnected(manager.connect(1000).test())
    }

    @Test
    fun `a connection failure disconnects and allows another attempt`() {
        val error = FailedToConnectException("Test connection failure")
        doThrow(error).doAnswer { null }.whenever(connection).connect(any())

        manager.connect(1000).test()
            .assertError { it === error }
            .assertNotComplete()
            .assertValues(PodEvent.BluetoothConnecting)

        verify(connection).disconnect(false)
        verify(connection, never()).establishSession(any(), any(), any(), any())
        verify(podState, never()).successfulConnections = any()
        verify(podState, never()).commitEapAkaSequenceNumber()
        assertConnected(manager.connect(1000).test())
        verify(connectionFactory).createConnection(address)
    }

    @Test
    fun `a session exception disconnects without committing DASH state and allows a retry`() {
        val error = SessionEstablishmentException("Test session failure")
        doAnswer { throw error }.whenever(connection).establishSession(any(), any(), any(), any())

        manager.connect(1000).test().assertError { it === error }.assertNotComplete()

        verify(connection).disconnect(false)
        verify(podState, never()).successfulConnections = any()
        verify(podState, never()).commitEapAkaSequenceNumber()
        doReturn(null).whenever(connection).establishSession(any(), any(), any(), any())
        assertConnected(manager.connect(1000).test())
    }

    @Test
    fun `a missing address fails before accessing Bluetooth and releases the busy lock`() {
        whenever(podState.bluetoothAddress).thenReturn(null)

        manager.connect(1000).test().assertError(FailedToConnectException::class.java).assertNotComplete()

        verifyNoInteractions(deviceManager, connectionFactory)
        whenever(podState.bluetoothAddress).thenReturn(address)
        assertConnected(manager.connect(1000).test())
    }

    @Test
    fun `unavailable Bluetooth fails before creating a connection and allows a retry`() {
        whenever(deviceManager.isBluetoothAvailable()).thenReturn(false)

        manager.connect(1000).test().assertError(ConnectException::class.java).assertNotComplete()

        verify(deviceManager, never()).ensureBondedIfRequired(any())
        verifyNoInteractions(connectionFactory)
        whenever(deviceManager.isBluetoothAvailable()).thenReturn(true)
        assertConnected(manager.connect(1000).test())
    }

    @Test
    fun `failed bonding prevents connecting and allows a retry`() {
        whenever(deviceManager.ensureBondedIfRequired(address)).thenReturn(false)

        manager.connect(1000).test().assertError(ConnectException::class.java).assertNotComplete()

        verifyNoInteractions(connectionFactory)
        whenever(deviceManager.ensureBondedIfRequired(address)).thenReturn(true)
        assertConnected(manager.connect(1000).test())
    }

    @Test
    fun `a missing key prevents session negotiation and allows a retry`() {
        whenever(podState.ltk).thenReturn(null)

        manager.connect(1000).test().assertError(FailedToConnectException::class.java).assertNotComplete()

        verify(connection).disconnect(false)
        verify(connection, never()).establishSession(any(), any(), any(), any())
        verify(podState, never()).increaseEapAkaSequenceNumber()
        verify(podState, never()).commitEapAkaSequenceNumber()
        whenever(podState.ltk).thenReturn(ltk)
        assertConnected(manager.connect(1000).test())
    }

    @Test
    fun `a successful command emits the original command and response and releases the busy lock`() {
        connectForCommand()

        assertCommandSuccess()
        assertCommandSuccess()

        verify(session, times(2)).sendCommand(command)
        verify(session, times(2)).readAndAckResponse()
        verify(connection, never()).disconnect(any())
    }

    @Test
    fun `an unconfirmed command still reads and emits the response`() {
        connectForCommand()
        whenever(session.sendCommand(command)).thenReturn(CommandSendErrorConfirming("Test confirmation failure"), CommandSendSuccess)

        manager.sendCommand(command, Response::class).test()
            .assertComplete()
            .assertNoErrors()
            .assertValueCount(3)
            .assertValueAt(0) { it is PodEvent.CommandSending && it.command === command }
            .assertValueAt(1) { it is PodEvent.CommandSendNotConfirmed && it.command === command }
            .assertValueAt(2) { it is PodEvent.ResponseReceived && it.command === command && it.response === response }

        verify(session).readAndAckResponse()
        verify(connection, never()).disconnect(any())
        assertCommandSuccess()
    }

    @Test
    fun `a response with a failed ACK is still emitted and releases the busy lock`() {
        connectForCommand()
        whenever(session.readAndAckResponse()).thenReturn(CommandAckError(response, "Test ACK failure"), CommandReceiveSuccess(response))

        assertCommandSuccess()
        assertCommandSuccess()

        verify(connection, never()).disconnect(any())
    }

    @Test
    fun `a send failure does not read a response and releases the busy lock`() {
        connectForCommand()
        whenever(session.sendCommand(command)).thenReturn(CommandSendErrorSending("Test send failure"), CommandSendSuccess)

        manager.sendCommand(command, Response::class).test()
            .assertError(CouldNotSendCommandException::class.java)
            .assertNotComplete()
            .assertValueCount(1)
            .assertValueAt(0) { it is PodEvent.CommandSending && it.command === command }

        verify(session, never()).readAndAckResponse()
        verify(connection, never()).disconnect(any())
        assertCommandSuccess()
    }

    @Test
    fun `a read failure emits no response and releases the busy lock`() {
        connectForCommand()
        whenever(session.readAndAckResponse()).thenReturn(CommandReceiveError("Test read failure"), CommandReceiveSuccess(response))

        manager.sendCommand(command, Response::class).test()
            .assertError(MessageIOException::class.java)
            .assertNotComplete()
            .assertValueCount(2)
            .assertValueAt(0) { it is PodEvent.CommandSending && it.command === command }
            .assertValueAt(1) { it is PodEvent.CommandSent && it.command === command }

        verify(connection, never()).disconnect(any())
        assertCommandSuccess()
    }

    @Test
    fun `a send exception disconnects and releases the busy lock`() {
        connectForCommand()
        val error = MessageIOException("Test send exception")
        doAnswer { throw error }.whenever(session).sendCommand(command)

        manager.sendCommand(command, Response::class).test()
            .assertError { it === error }
            .assertNotComplete()
            .assertValueCount(1)

        verify(session, never()).readAndAckResponse()
        verify(connection).disconnect(false)
        doReturn(CommandSendSuccess).whenever(session).sendCommand(command)
        assertConnected(manager.connect(1000).test())
        assertCommandSuccess()
    }

    @Test
    fun `a read exception disconnects and releases the busy lock`() {
        connectForCommand()
        val error = MessageIOException("Test read exception")
        doAnswer { throw error }.whenever(session).readAndAckResponse()

        manager.sendCommand(command, Response::class).test()
            .assertError { it === error }
            .assertNotComplete()
            .assertValueCount(2)

        verify(connection).disconnect(false)
        doReturn(CommandReceiveSuccess(response)).whenever(session).readAndAckResponse()
        assertConnected(manager.connect(1000).test())
        assertCommandSuccess()
    }

    @Test
    fun `a command without a session fails before sending and allows a later command`() {
        connectForCommand()
        whenever(connection.session).thenReturn(null)

        manager.sendCommand(command, Response::class).test()
            .assertError(NotConnectedException::class.java)
            .assertNotComplete()
            .assertNoValues()

        verifyNoInteractions(session)
        verify(connection).disconnect(false)
        whenever(connection.session).thenReturn(session)
        assertConnected(manager.connect(1000).test())
        assertCommandSuccess()
    }

    @Test
    fun `a busy command rejects other operations without releasing the owners lock`() {
        connectForCommand()
        doAnswer {
            manager.sendCommand(command, Response::class).test().assertError(BusyException::class.java).assertNoValues()
            manager.connect(1000).test().assertError(BusyException::class.java).assertNoValues()
            manager.pairNewPod().test().assertError(BusyException::class.java).assertNoValues()
            manager.sendCommand(command, Response::class).test().assertError(BusyException::class.java).assertNoValues()
            CommandSendSuccess
        }.whenever(session).sendCommand(command)

        assertCommandSuccess()

        verify(session).sendCommand(command)
        verify(session).readAndAckResponse()
        verify(connection, never()).connect(any())
        verify(connection, never()).disconnect(any())
        doReturn(CommandSendSuccess).whenever(session).sendCommand(command)
        assertCommandSuccess()
    }

    @Test
    fun `a busy connection rejects another attempt until it has finished`() {
        doAnswer {
            manager.connect(1000).test().assertError(BusyException::class.java).assertNoValues()
            manager.sendCommand(command, Response::class).test().assertError(BusyException::class.java).assertNoValues()
            manager.connect(1000).test().assertError(BusyException::class.java).assertNoValues()
            null
        }.whenever(connection).connect(any())

        assertConnected(manager.connect(1000).test())

        verify(connection).connect(any())
        assertCommandSuccess()
    }

    @Test
    fun `an already paired DASH pod does not scan and releases the busy lock`() {
        manager.pairNewPod().test().assertResult(PodEvent.AlreadyPaired)
        manager.pairNewPod().test().assertResult(PodEvent.AlreadyPaired)

        verifyNoInteractions(deviceManager, connectionFactory)
        assertConnected(manager.connect(1000).test())
    }

    @Test
    fun `a scan failure is reported without creating a connection and releases the busy lock`() {
        val scanner: PodScanner = mock()
        val error = ConnectException("Test scan failure")
        whenever(podState.ltk).thenReturn(null)
        whenever(deviceManager.createPodScanner()).thenReturn(scanner)
        doAnswer { throw error }.whenever(scanner).scanForPod(PodScanner.SCAN_FOR_SERVICE_UUID, PodScanner.POD_ID_NOT_ACTIVATED)

        manager.pairNewPod().test().assertValues(PodEvent.Scanning).assertError { it === error }.assertNotComplete()

        verifyNoInteractions(connectionFactory)
        whenever(podState.ltk).thenReturn(ltk)
        manager.pairNewPod().test().assertResult(PodEvent.AlreadyPaired)
    }

    @Test
    fun `a new pod whose service is missing is retried once on a fresh connection`() {
        val first: BleConnection = mock()
        val second: BleConnection = mock()
        prepareNewPodScan()
        whenever(connectionFactory.createConnection(address)).thenReturn(first, second)
        doAnswer { throw ConnectException("Service not found: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f") }.whenever(first).connect(any())

        // The second connection has no message IO in this test, so pairing stops right after it connects.
        manager.pairNewPod().test()
            .assertValueCount(4)
            .assertValueAt(0, PodEvent.Scanning)
            .assertValueAt(1, PodEvent.BluetoothConnecting)
            .assertValueAt(2) { it is PodEvent.BluetoothConnected && it.bluetoothAddress == address }
            .assertValueAt(3, PodEvent.Pairing)
            .assertError { it is ConnectException && it.msg == "Connection lost" }

        val waitCondition = ConnectionWaitCondition(timeoutMs = BleConnection.DEFAULT_CONNECT_TIMEOUT_MS)
        val firstCondition = argumentCaptor<ConnectionWaitCondition>()
        val secondCondition = argumentCaptor<ConnectionWaitCondition>()
        inOrder(first, second) {
            verify(first).connect(firstCondition.capture())
            verify(first).disconnect(true)
            verify(second).connect(secondCondition.capture())
            verify(second).disconnect(false)
        }
        assertThat(firstCondition.firstValue).isEqualTo(waitCondition)
        assertThat(secondCondition.firstValue).isEqualTo(waitCondition)
        // connect() shortens the timeout it is given, so the reconnect needs its own condition.
        assertThat(secondCondition.firstValue).isNotSameInstanceAs(firstCondition.firstValue)
        verify(connectionFactory, times(2)).createConnection(address)
    }

    @Test
    fun `a new pod whose service is missing twice fails after one reconnect and releases the busy lock`() {
        val first: BleConnection = mock()
        val second: BleConnection = mock()
        prepareNewPodScan()
        whenever(connectionFactory.createConnection(address)).thenReturn(first, second)
        doAnswer { throw ConnectException("Service not found: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f") }.whenever(first).connect(any())
        doAnswer { throw ConnectException("Service not found: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f") }.whenever(second).connect(any())

        manager.pairNewPod().test()
            .assertValues(PodEvent.Scanning, PodEvent.BluetoothConnecting)
            .assertError { it is ConnectException && it.msg == "Service not found: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f (after one reconnect)" }

        verify(connectionFactory, times(2)).createConnection(address)
        verify(first).disconnect(true)
        verify(second).disconnect(false)
        verify(second, never()).disconnect(true)
        whenever(podState.ltk).thenReturn(ltk)
        manager.pairNewPod().test().assertResult(PodEvent.AlreadyPaired)
    }

    @Test
    fun `other new pod connection failures are not retried`() {
        val error = ConnectException("connection timeout")
        prepareNewPodScan()
        doAnswer { throw error }.whenever(connection).connect(any())

        manager.pairNewPod().test()
            .assertValues(PodEvent.Scanning, PodEvent.BluetoothConnecting)
            .assertError { it === error }

        verify(connectionFactory).createConnection(address)
        verify(connection).connect(any())
        verify(connection).disconnect(false)
        verify(connection, never()).disconnect(true)
    }

    @Test
    fun `a new pod whose service discovery did not complete is not retried`() {
        val error = ConnectException("Service discovery did not complete: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f")
        prepareNewPodScan()
        doAnswer { throw error }.whenever(connection).connect(any())

        manager.pairNewPod().test()
            .assertValues(PodEvent.Scanning, PodEvent.BluetoothConnecting)
            .assertError { it === error }

        verify(connectionFactory).createConnection(address)
        verify(connection, never()).disconnect(true)
    }

    @Test
    fun `a new pod failing to connect is not retried`() {
        val error = FailedToConnectException(address)
        prepareNewPodScan()
        doAnswer { throw error }.whenever(connection).connect(any())

        manager.pairNewPod().test()
            .assertValues(PodEvent.Scanning, PodEvent.BluetoothConnecting)
            .assertError { it === error }

        verify(connectionFactory).createConnection(address)
        verify(connection, never()).disconnect(true)
    }

    @Test
    fun `a cancelled activation is not retried when the service is missing`() {
        val error = ConnectException("Service not found: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f")
        val observer = TestObserver<PodEvent>()
        prepareNewPodScan()
        doAnswer {
            observer.dispose()
            throw error
        }.whenever(connection).connect(any())

        manager.pairNewPod().subscribe(observer)

        observer.assertValues(PodEvent.Scanning, PodEvent.BluetoothConnecting).assertNotComplete()
        verify(connectionFactory).createConnection(address)
        verify(connection).disconnect(false)
        verify(connection, never()).disconnect(true)
        whenever(podState.ltk).thenReturn(ltk)
        manager.pairNewPod().test().assertResult(PodEvent.AlreadyPaired)
    }

    @Test
    fun `an activation cancelled while closing the first connection is not retried`() {
        val first: BleConnection = mock()
        val second: BleConnection = mock()
        val observer = TestObserver<PodEvent>()
        prepareNewPodScan()
        whenever(connectionFactory.createConnection(address)).thenReturn(first, second)
        doAnswer { throw ConnectException("Service not found: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f") }.whenever(first).connect(any())
        doAnswer { observer.dispose() }.whenever(first).disconnect(true)

        manager.pairNewPod().subscribe(observer)

        observer.assertValues(PodEvent.Scanning, PodEvent.BluetoothConnecting).assertNotComplete()
        verify(connectionFactory).createConnection(address)
        verify(second, never()).connect(any())
        whenever(podState.ltk).thenReturn(ltk)
        manager.pairNewPod().test().assertResult(PodEvent.AlreadyPaired)
    }

    @Test
    fun `an activation cancelled during the reconnect does not pair`() {
        val first: BleConnection = mock()
        val second: BleConnection = mock()
        val observer = TestObserver<PodEvent>()
        prepareNewPodScan()
        whenever(connectionFactory.createConnection(address)).thenReturn(first, second)
        doAnswer { throw ConnectException("Service not found: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f") }.whenever(first).connect(any())
        doAnswer { observer.dispose() }.whenever(second).connect(any())

        manager.pairNewPod().subscribe(observer)

        observer.assertValues(PodEvent.Scanning, PodEvent.BluetoothConnecting).assertNotComplete()
        verify(second).disconnect(false)
        verify(second, never()).msgIO
        whenever(podState.ltk).thenReturn(ltk)
        manager.pairNewPod().test().assertResult(PodEvent.AlreadyPaired)
    }

    @Test
    fun `another service error of the reconnect is reported unchanged`() {
        val first: BleConnection = mock()
        val second: BleConnection = mock()
        val error = ConnectException("Characteristic not found: CMD")
        prepareNewPodScan()
        whenever(connectionFactory.createConnection(address)).thenReturn(first, second)
        doAnswer { throw ConnectException("Service not found: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f") }.whenever(first).connect(any())
        doAnswer { throw error }.whenever(second).connect(any())

        manager.pairNewPod().test()
            .assertValues(PodEvent.Scanning, PodEvent.BluetoothConnecting)
            .assertError { it === error }

        verify(connectionFactory, times(2)).createConnection(address)
    }

    @Test
    fun `another failure of the reconnect is reported unchanged`() {
        val first: BleConnection = mock()
        val second: BleConnection = mock()
        val error = FailedToConnectException(address)
        prepareNewPodScan()
        whenever(connectionFactory.createConnection(address)).thenReturn(first, second)
        doAnswer { throw ConnectException("Service not found: 1a7e-4024-e3ed-4464-8b7e-751e03d0dc5f") }.whenever(first).connect(any())
        doAnswer { throw error }.whenever(second).connect(any())

        manager.pairNewPod().test()
            .assertValues(PodEvent.Scanning, PodEvent.BluetoothConnecting)
            .assertError { it === error }

        verify(connectionFactory, times(2)).createConnection(address)
        verify(first).disconnect(true)
        verify(second).disconnect(false)
    }

    private fun prepareNewPodScan() {
        val scanner: PodScanner = mock()
        val device: BleDiscoveredDevice = mock()
        whenever(device.address).thenReturn(address)
        whenever(podState.ltk).thenReturn(null)
        whenever(deviceManager.createPodScanner()).thenReturn(scanner)
        whenever(scanner.scanForPod(PodScanner.SCAN_FOR_SERVICE_UUID, PodScanner.POD_ID_NOT_ACTIVATED)).thenReturn(device)
    }

    private fun connectForCommand() {
        assertConnected(manager.connect(1000).test())
        clearInvocations(connection, connectionFactory, podState, deviceManager, session)
    }

    private fun assertConnected(observer: TestObserver<PodEvent>) {
        observer.assertComplete()
            .assertNoErrors()
            .assertValueCount(4)
            .assertValueAt(0, PodEvent.BluetoothConnecting)
            .assertValueAt(1) { it is PodEvent.BluetoothConnected && it.bluetoothAddress == address }
            .assertValueAt(2, PodEvent.EstablishingSession)
            .assertValueAt(3, PodEvent.Connected)
    }

    private fun assertCommandSuccess() {
        manager.sendCommand(command, Response::class).test()
            .assertComplete()
            .assertNoErrors()
            .assertValueCount(3)
            .assertValueAt(0) { it is PodEvent.CommandSending && it.command === command }
            .assertValueAt(1) { it is PodEvent.CommandSent && it.command === command }
            .assertValueAt(2) { it is PodEvent.ResponseReceived && it.command === command && it.response === response }
    }
}
