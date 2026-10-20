package app.aaps.pump.omnipod.common.bledriver.comm.legacy.session

import android.bluetooth.BluetoothGatt
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.pump.omnipod.common.bledriver.comm.exceptions.ConnectException
import app.aaps.pump.omnipod.common.bledriver.comm.interfaces.session.BleConnection
import app.aaps.pump.omnipod.common.bledriver.comm.legacy.callbacks.BleCommCallbacks
import app.aaps.pump.omnipod.common.bledriver.comm.session.ConnectionWaitCondition
import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ServiceDiscovererTest {

    private val gatt: BluetoothGatt = mock()
    private val callbacks: BleCommCallbacks = mock()
    private val discoverer = ServiceDiscoverer(mock<AAPSLogger>(), gatt, callbacks, mock<BleConnection>())

    @BeforeEach
    fun setUp() {
        whenever(gatt.discoverServices()).thenReturn(true)
        whenever(gatt.getService(any())).thenReturn(null)
        whenever(gatt.services).thenReturn(emptyList())
    }

    @Test
    fun `a completed discovery without the service reports service not found`() {
        whenever(callbacks.waitForServiceDiscovery(any())).thenReturn(true)

        val ex = assertThrows<ConnectException> { discoverer.discoverServices(ConnectionWaitCondition(timeoutMs = 1000)) }

        assertThat(ex.msg).startsWith(BleConnection.SERVICE_NOT_FOUND)
    }

    @Test
    fun `a discovery that did not complete is not reported as service not found`() {
        whenever(callbacks.waitForServiceDiscovery(any())).thenReturn(false)

        val ex = assertThrows<ConnectException> { discoverer.discoverServices(ConnectionWaitCondition(timeoutMs = 1000)) }

        assertThat(ex.msg).startsWith("Service discovery did not complete")
        assertThat(ex.msg).doesNotContain(BleConnection.SERVICE_NOT_FOUND)
    }
}
