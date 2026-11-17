package moe.rukamori.archivetune.downloads

import android.content.Context
import android.os.SystemClock
import androidx.media3.exoplayer.offline.Download
import androidx.media3.exoplayer.offline.DownloadService
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.transformLatest
import moe.rukamori.archivetune.db.MusicDatabase
import moe.rukamori.archivetune.db.entities.Album
import moe.rukamori.archivetune.db.entities.Playlist
import moe.rukamori.archivetune.db.entities.PlaylistSongMap
import moe.rukamori.archivetune.db.entities.Song
import moe.rukamori.archivetune.download.DownloadUtil
import moe.rukamori.archivetune.playback.ExoDownloadService
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Salinan immutable dari progres satu unduhan yang belum selesai.
 *
 * Media3 memperbarui byte yang terunduh langsung di dalam objek [Download], tanpa membuat objek
 * baru dan tanpa memanggil listener, sehingga StateFlow/Compose tidak pernah melihat perubahannya.
 * Data class ini dibuat ulang di setiap tick, jadi UI pasti ter-update.
 */
data class DownloadProgressInfo(
    val state: Int,
    val bytesDownloaded: Long,
    val contentLength: Long,
    val percentDownloaded: Float,
    val speedBytesPerSecond: Long,
)

data class DownloadRepositorySnapshot(
    val songs: List<Song>,
    val albums: List<Album>,
    val playlists: List<Playlist>,
    val playlistSongMaps: List<PlaylistSongMap>,
    val downloads: Map<String, Download>,
    /** Kunci: songId. Hanya berisi unduhan yang belum selesai; untuk yang selesai pakai [downloads]. */
    val progress: Map<String, DownloadProgressInfo> = emptyMap(),
)

interface DownloadRepository {
    fun observeDownloads(): Flow<DownloadRepositorySnapshot>

    fun pause(songIds: Collection<String>)

    fun resume(songIds: Collection<String>)

    fun remove(songIds: Collection<String>)
}

@Singleton
class Media3DownloadRepository
    @Inject
    constructor(
        @ApplicationContext private val context: Context,
        private val database: MusicDatabase,
        private val downloadUtil: DownloadUtil,
    ) : DownloadRepository {
        // Sengaja TANPA onStart { emit(emptyList()) }: combine baru mengeluarkan snapshot pertama
        // setelah semua sumber siap, jadi daftar muncul utuh sekaligus (tidak bertahap / loncat).
        override fun observeDownloads(): Flow<DownloadRepositorySnapshot> =
            combine(
                database.allSongs(),
                database.allAlbumsForDownloads(),
                database.allPlaylistsForDownloads(),
                database.allPlaylistSongMapsForDownloads(),
                observeDownloadsTick(),
            ) { songs, albums, playlists, playlistSongMaps, tick ->
                DownloadRepositorySnapshot(
                    songs = songs,
                    albums = albums,
                    playlists = playlists,
                    playlistSongMaps = playlistSongMaps,
                    downloads = tick.downloads,
                    progress = tick.progress,
                )
            }.flowOn(Dispatchers.IO)
                // Kalau UI lebih lambat dari sumber data, selalu ambil snapshot terbaru.
                .conflate()

        private fun observeDownloadsTick(): Flow<DownloadsTick> =
            flow {
                // Tracker dibuat per collector agar data kecepatan tidak tercampur antar subscriber.
                val speedTracker = DownloadSpeedTracker()
                emitAll(
                    downloadUtil.downloads
                        .tickWhileDownloading(PROGRESS_INTERVAL_MS)
                        .map { downloads ->
                            DownloadsTick(
                                downloads = downloads,
                                progress = speedTracker.update(downloads, SystemClock.elapsedRealtime()),
                            )
                        },
                )
            }

        override fun pause(songIds: Collection<String>) {
            songIds.distinct().forEach { songId ->
                DownloadService.sendSetStopReason(
                    context,
                    ExoDownloadService::class.java,
                    songId,
                    PAUSED_STOP_REASON,
                    false,
                )
            }
        }

        override fun resume(songIds: Collection<String>) {
            songIds.distinct().forEach { songId ->
                val download = downloadUtil.downloads.value[songId] ?: return@forEach
                when (download.state) {
                    Download.STATE_FAILED ->
                        DownloadService.sendAddDownload(
                            context,
                            ExoDownloadService::class.java,
                            download.request,
                            NO_STOP_REASON,
                            true,
                        )

                    Download.STATE_STOPPED ->
                        DownloadService.sendSetStopReason(
                            context,
                            ExoDownloadService::class.java,
                            songId,
                            NO_STOP_REASON,
                            true,
                        )
                }
            }
        }

        override fun remove(songIds: Collection<String>) {
            songIds.distinct().forEach { songId ->
                DownloadService.sendRemoveDownload(
                    context,
                    ExoDownloadService::class.java,
                    songId,
                    false,
                )
            }
        }

        private companion object {
            const val NO_STOP_REASON = 0
            const val PAUSED_STOP_REASON = 1
            const val PROGRESS_INTERVAL_MS = 500L
        }
    }

private data class DownloadsTick(
    val downloads: Map<String, Download>,
    val progress: Map<String, DownloadProgressInfo>,
)

/**
 * `downloads` dari DownloadUtil hanya berubah saat *status* unduhan berubah, bukan saat byte
 * bertambah. Operator seperti sample() tidak membantu di sini karena tidak ada emisi baru yang bisa
 * disampel. Fungsi ini menerbitkan ulang map yang sama setiap [intervalMs] selama masih ada unduhan
 * yang berjalan, supaya progres dan kecepatan bisa dibaca ulang. Emisi pertama keluar langsung
 * tanpa jeda, dan ticker otomatis berhenti begitu tidak ada lagi unduhan aktif.
 */
@OptIn(ExperimentalCoroutinesApi::class)
private fun Flow<Map<String, Download>>.tickWhileDownloading(intervalMs: Long): Flow<Map<String, Download>> =
    transformLatest { downloads ->
        emit(downloads)
        if (downloads.values.any { it.state == Download.STATE_DOWNLOADING }) {
            while (true) {
                delay(intervalMs)
                emit(downloads)
            }
        }
    }

/** Menghitung kecepatan unduhan (byte/detik) dari selisih byte antar tick, lalu dihaluskan. */
private class DownloadSpeedTracker {
    private class Sample(
        val bytes: Long,
        val timeMs: Long,
        val speed: Long,
    )

    private val samples = HashMap<String, Sample>()

    fun update(
        downloads: Map<String, Download>,
        nowMs: Long,
    ): Map<String, DownloadProgressInfo> {
        samples.keys.retainAll { downloads[it]?.state == Download.STATE_DOWNLOADING }

        val result = HashMap<String, DownloadProgressInfo>()
        for ((id, download) in downloads) {
            if (download.state == Download.STATE_COMPLETED) continue
            val bytes = download.bytesDownloaded
            val speed =
                if (download.state == Download.STATE_DOWNLOADING) {
                    measureSpeed(id, bytes, nowMs)
                } else {
                    0L
                }
            result[id] =
                DownloadProgressInfo(
                    state = download.state,
                    bytesDownloaded = bytes,
                    contentLength = download.contentLength,
                    percentDownloaded = download.percentDownloaded,
                    speedBytesPerSecond = speed,
                )
        }
        return result
    }

    private fun measureSpeed(
        id: String,
        bytes: Long,
        nowMs: Long,
    ): Long {
        val previous = samples[id]
        if (previous == null) {
            samples[id] = Sample(bytes, nowMs, 0L)
            return 0L
        }

        val elapsedMs = nowMs - previous.timeMs
        // Emisi yang terlalu berdekatan (mis. karena perubahan status) bikin angka melonjak.
        if (elapsedMs < MIN_SAMPLE_WINDOW_MS) return previous.speed

        val instantSpeed = (bytes - previous.bytes).coerceAtLeast(0L) * 1000L / elapsedMs
        val speed = if (previous.speed == 0L) instantSpeed else (previous.speed + instantSpeed) / 2
        samples[id] = Sample(bytes, nowMs, speed)
        return speed
    }

    private companion object {
        const val MIN_SAMPLE_WINDOW_MS = 250L
    }
}
