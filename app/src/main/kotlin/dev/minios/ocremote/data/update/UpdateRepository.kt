package dev.minios.ocremote.data.update

import androidx.datastore.core.DataStore
import androidx.datastore.preferences.core.Preferences
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.longPreferencesKey
import androidx.datastore.preferences.core.stringPreferencesKey
import dev.minios.ocremote.BuildConfig
import dev.minios.ocremote.logging.AppLogger as Log
import io.ktor.client.HttpClient
import io.ktor.client.plugins.timeout
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.HttpHeaders
import io.ktor.http.isSuccess
import io.ktor.utils.io.readAvailable
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import java.io.ByteArrayOutputStream
import javax.inject.Inject
import javax.inject.Singleton

private const val RICH_UPDATE_MANIFEST_URL = "https://github.com/crim50n/oc-remote/releases/latest/download/update.json"
private const val UPDATE_MANIFEST_URL = "https://raw.githubusercontent.com/crim50n/oc-remote/master/update.json"
private const val GITHUB_LATEST_RELEASE_URL = "https://api.github.com/repos/crim50n/oc-remote/releases/latest"
private const val MAX_RESPONSE_BYTES = 64 * 1024L
private const val MAX_MANIFEST_CHARS = 8 * 1024
private const val MAX_GITHUB_CHARS = 64 * 1024
private const val UPDATE_TIMEOUT_MS = 15_000L
private const val TAG = "UpdateRepository"

@Singleton
class UpdateRepository @Inject constructor(
    private val client: HttpClient,
    private val json: Json,
    private val dataStore: DataStore<Preferences>,
) {
    private val _state = MutableStateFlow<UpdateState>(UpdateState.Idle)
    val state: StateFlow<UpdateState> = _state.asStateFlow()
    private val operationMutex = Mutex()
    private val notesMutex = Mutex()

    suspend fun restore() = operationMutex.withLock {
        if (_state.value is UpdateState.Downloading || _state.value is UpdateState.ReadyToInstall) return@withLock
        val preferences = dataStore.data.first()
        decodeCachedRelease(preferences[CACHED_RELEASE_KEY])
            ?.takeIf { UpdatePolicy.isNewer(it, BuildConfig.VERSION_CODE, BuildConfig.VERSION_NAME) }
            ?.let { _state.value = UpdateState.Available(it) }
    }

    suspend fun check(manual: Boolean) = operationMutex.withLock {
        if (_state.value is UpdateState.Downloading || _state.value is UpdateState.ReadyToInstall) return@withLock
        val preferences = dataStore.data.first()
        val now = System.currentTimeMillis()
        if (!manual && !UpdatePolicy.shouldCheckAutomatically(now, preferences[LAST_ATTEMPT_KEY] ?: 0L)) return
        val cachedAvailable = _state.value as? UpdateState.Available
        if (manual || cachedAvailable == null) _state.value = UpdateState.Checking
        val discovered = try {
            fetchUpdateOrNull { fetchManifest(RICH_UPDATE_MANIFEST_URL) }
                ?: fetchUpdateOrNull { fetchManifest(UPDATE_MANIFEST_URL) }
                ?: fetchUpdateOrNull { fetchLatestGitHubRelease() }
        } catch (e: CancellationException) {
            _state.value = cachedAvailable ?: UpdateState.Idle
            throw e
        }
        // Like Telegram standalone, throttle completed checks, including network failures.
        // Cancellation is not a completed check and must not postpone the next resume attempt.
        dataStore.edit { it[LAST_ATTEMPT_KEY] = System.currentTimeMillis() }
        if (discovered == null) {
            _state.value = cachedAvailable ?: if (manual) {
                UpdateState.Error("Unable to check for updates")
            } else {
                UpdateState.Idle
            }
            return
        }
        val cached = decodeCachedRelease(preferences[CACHED_RELEASE_KEY])
        val release = if (cached?.versionName == discovered.versionName && discovered.releaseNotes == null) {
            discovered.copy(releaseNotes = cached.releaseNotes, publishedAt = cached.publishedAt)
        } else discovered

        if (UpdatePolicy.isNewer(release, BuildConfig.VERSION_CODE, BuildConfig.VERSION_NAME)) {
            dataStore.edit { it[CACHED_RELEASE_KEY] = json.encodeToString(AvailableUpdate.serializer(), release) }
            _state.value = UpdateState.Available(release)
        } else {
            dataStore.edit { it.remove(CACHED_RELEASE_KEY) }
            _state.value = UpdateState.UpToDate
        }
    }

    private suspend fun fetchUpdateOrNull(fetch: suspend () -> AvailableUpdate): AvailableUpdate? = try {
        fetch()
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        Log.e(TAG, "Update source check failed", e)
        null
    }

    private suspend fun fetchManifest(url: String): AvailableUpdate {
        val response = client.get(url) {
            timeout { requestTimeoutMillis = UPDATE_TIMEOUT_MS }
        }
        response.requireBoundedSuccess()
        val dto = json.decodeFromString<UpdateManifestDto>(response.readBounded(MAX_MANIFEST_CHARS))
        return requireNotNull(UpdatePolicy.manifestToRelease(dto)) { "Invalid update manifest" }
    }

    private suspend fun fetchLatestGitHubRelease(): AvailableUpdate {
        val response = client.get(GITHUB_LATEST_RELEASE_URL) {
            header(HttpHeaders.Accept, "application/vnd.github+json")
            header(HttpHeaders.UserAgent, "OC-Remote/${BuildConfig.VERSION_NAME}")
            timeout { requestTimeoutMillis = UPDATE_TIMEOUT_MS }
        }
        response.requireBoundedSuccess()
        val dto = json.decodeFromString<GitHubReleaseDto>(response.readBounded(MAX_GITHUB_CHARS))
        return requireNotNull(UpdatePolicy.githubToRelease(dto)) { "Invalid GitHub release" }
    }

    private fun decodeCachedRelease(value: String?): AvailableUpdate? {
        if (value.isNullOrBlank() || value.length > 256 * 1024) return null
        return runCatching { json.decodeFromString(AvailableUpdate.serializer(), value) }
            .getOrNull()
            ?.takeIf(UpdatePolicy::isInstallableOrDiscoveryOnly)
    }

    suspend fun loadReleaseNotes(release: AvailableUpdate): AvailableUpdate = notesMutex.withLock {
        val cached = decodeCachedRelease(dataStore.data.first()[CACHED_RELEASE_KEY])
        if (cached?.versionName == release.versionName && cached.releaseNotes != null) return@withLock cached
        require(UpdatePolicy.hasValidReleaseUrl(release))
        val response = client.get("https://api.github.com/repos/crim50n/oc-remote/releases/tags/v${release.versionName}") {
            header(HttpHeaders.Accept, "application/vnd.github+json")
            header(HttpHeaders.UserAgent, "OC-Remote/${BuildConfig.VERSION_NAME}")
            timeout { requestTimeoutMillis = UPDATE_TIMEOUT_MS }
        }
        response.requireBoundedSuccess()
        val dto = json.decodeFromString<GitHubReleaseDto>(response.readBounded(MAX_GITHUB_CHARS))
        val enriched = UpdatePolicy.withReleaseNotes(release, dto)
        dataStore.edit { preferences ->
            val current = decodeCachedRelease(preferences[CACHED_RELEASE_KEY])
            if (current?.versionName == release.versionName) {
                preferences[CACHED_RELEASE_KEY] = json.encodeToString(AvailableUpdate.serializer(),
                    current.copy(releaseNotes = enriched.releaseNotes, publishedAt = enriched.publishedAt))
            }
        }
        _state.update { state ->
            fun AvailableUpdate.enrich() = copy(releaseNotes = enriched.releaseNotes, publishedAt = enriched.publishedAt)
            when {
                state is UpdateState.Downloading && state.release.versionName == release.versionName ->
                    UpdateState.Downloading(state.release.enrich(), state.progressPercent)
                state is UpdateState.ReadyToInstall && state.release.versionName == release.versionName ->
                    UpdateState.ReadyToInstall(state.release.enrich(), state.apkPath)
                state is UpdateState.Available && state.release.versionName == release.versionName ->
                    UpdateState.Available(state.release.enrich())
                state is UpdateState.Error && state.release?.versionName == release.versionName ->
                    state.copy(release = state.release.enrich())
                else -> state
            }
        }
        enriched
    }

    private fun HttpResponse.requireBoundedSuccess() {
        require(status.isSuccess()) { "Update server returned $status" }
        val contentLength = headers[HttpHeaders.ContentLength]?.toLongOrNull()
        require(contentLength == null || contentLength in 1..MAX_RESPONSE_BYTES) { "Update response is too large" }
    }

    private suspend fun HttpResponse.readBounded(maxChars: Int): String {
        val channel = bodyAsChannel()
        val output = ByteArrayOutputStream(minOf(maxChars, 8 * 1024))
        val buffer = ByteArray(1024)
        var total = 0
        while (true) {
            val count = channel.readAvailable(buffer)
            if (count < 0) break
            if (count == 0) continue
            total += count
            require(total <= maxChars) { "Update response is too large" }
            output.write(buffer, 0, count)
        }
        return output.toString(Charsets.UTF_8.name())
    }

    private companion object {
        val LAST_ATTEMPT_KEY = longPreferencesKey("update_last_attempt")
        val CACHED_RELEASE_KEY = stringPreferencesKey("update_cached_release")
    }
}
