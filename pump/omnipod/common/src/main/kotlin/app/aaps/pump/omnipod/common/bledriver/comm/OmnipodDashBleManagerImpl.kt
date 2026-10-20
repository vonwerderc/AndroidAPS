package app.aaps.pump.omnipod.common.bledriver.comm

import app.aaps.core.interfaces.configuration.Config
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.ConnectException
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.device.BleDeviceManager
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.scan.PodScanner
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.session.BleConnection
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.session.BleConnectionFactory
import app.aaps.pump.omnipod.common.bledriver.comm.pair.LTKExchanger
import app.aaps.pump.omnipod.common.bledriver.comm.session.ConnectionWaitCondition
import app.aaps.pump.omnipod.common.bledriver.event.PodEvent
import app.aaps.pump.omnipod.common.bledriver.pod.state.OmnipodDashPodStateManager
import dev.zacsweers.metro.Inject
import io.reactivex.rxjava3.core.Observable

@Inject
class OmnipodDashBleManagerImpl(
    aapsLogger: AAPSLogger,
    private val podState: OmnipodDashPodStateManager,
    private val config: Config,
    private val bleConnectionFactory: BleConnectionFactory,
    bleDeviceManager: BleDeviceManager,
) : SharedBleManager(aapsLogger, bleDeviceManager) {

    override val bluetoothAddress: String?
        get() = podState.bluetoothAddress
    override val ltk: ByteArray?
        get() = podState.ltk
    override val ids = Ids(podState)

    override fun createConnection(podAddress: String): BleConnection =
        bleConnectionFactory.createConnection(podAddress)

    override fun increaseEapAkaSequenceNumber(): ByteArray = podState.increaseEapAkaSequenceNumber()

    override fun updateEapAkaSequenceNumber(sequenceNumber: Long) {
        podState.eapAkaSequenceNumber = sequenceNumber
    }

    override fun commitEapAkaSequenceNumber() {
        podState.commitEapAkaSequenceNumber()
    }

    override fun recordSuccessfulConnection() {
        podState.successfulConnections++
    }

    override fun pairNewPod(): Observable<PodEvent> = Observable.create { emitter ->
        acquireBusy()
        try {
            if (podState.ltk != null) {
                emitter.onNext(PodEvent.AlreadyPaired)
                emitter.onComplete()
                return@create
            }
            aapsLogger.info(LTag.PUMPBTCOMM, "Starting new pod activation")
            emitter.onNext(PodEvent.Scanning)
            val podAddress = bleDeviceManager.createPodScanner().scanForPod(
                PodScanner.SCAN_FOR_SERVICE_UUID,
                PodScanner.POD_ID_NOT_ACTIVATED
            ).address
            podState.bluetoothAddress = podAddress

            emitter.onNext(PodEvent.BluetoothConnecting)
            val conn = connectNewPod(podAddress) { emitter.isDisposed }
            emitter.onNext(PodEvent.BluetoothConnected(podAddress))

            emitter.onNext(PodEvent.Pairing)
            val messageIO = conn.msgIO ?: throw ConnectException("Connection lost")
            val pairResult = LTKExchanger(aapsLogger, config, messageIO, ids).negotiateLTK()
            emitter.onNext(PodEvent.Paired(ids.podId))
            podState.updateFromPairing(ids.podId, pairResult)
            emitter.onNext(PodEvent.EstablishingSession)
            establishSession(pairResult.msgSeq)
            emitter.onNext(PodEvent.Connected)
            emitter.onComplete()
        } catch (ex: Exception) {
            disconnect(false)
            emitter.tryOnError(ex)
        } finally {
            releaseBusy()
        }
    }

    // On some phones (seen on a OnePlus with Android 16) the first service discovery on a newly established link
    // completes with status 0 and an empty service list, while reconnecting right away, before the stack drops the
    // idle link (about a second), finds the service. Routine connections get that quick reconnect from their next
    // attempt; activation rescans before every manual retry, so a new pod gets one immediate retry here instead.
    // Keep the delay well under a second, or the retry starts on a new link again.
    private fun connectNewPod(podAddress: String, isCancelled: () -> Boolean): BleConnection {
        val first = createConnection(podAddress)
        connection = first
        try {
            first.connect(ConnectionWaitCondition(timeoutMs = BleConnection.DEFAULT_CONNECT_TIMEOUT_MS))
            return first
        } catch (ex: ConnectException) {
            if (!ex.msg.startsWith(BleConnection.SERVICE_NOT_FOUND) || isCancelled()) throw ex
            aapsLogger.warn(LTag.PUMPBTCOMM, "DASH service not found on new pod; reconnecting once")
        }
        first.disconnect(true)
        Thread.sleep(MISSING_SERVICE_RETRY_DELAY_MS)
        if (isCancelled()) throw ConnectException("Pod activation cancelled")
        val second = createConnection(podAddress)
        connection = second
        try {
            second.connect(ConnectionWaitCondition(timeoutMs = BleConnection.DEFAULT_CONNECT_TIMEOUT_MS))
        } catch (ex: ConnectException) {
            if (ex.msg.startsWith(BleConnection.SERVICE_NOT_FOUND)) throw ConnectException("${ex.msg} (after one reconnect)")
            throw ex
        }
        if (isCancelled()) throw ConnectException("Pod activation cancelled")
        aapsLogger.info(LTag.PUMPBTCOMM, "DASH service found on reconnect")
        return second
    }

    companion object {
        const val CONTROLLER_ID = 4242 // TODO read from preferences or somewhere else.

        private const val MISSING_SERVICE_RETRY_DELAY_MS = 200L
    }
}
