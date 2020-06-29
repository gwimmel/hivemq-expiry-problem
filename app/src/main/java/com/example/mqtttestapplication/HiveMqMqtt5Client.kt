package com.example.mqtttestapplication

import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.MqttGlobalPublishFilter
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.mqtt5.Mqtt5RxClient

import io.reactivex.Completable
import io.reactivex.Observable

import io.reactivex.disposables.CompositeDisposable

import timber.log.Timber
import java.util.concurrent.TimeUnit

class HiveMqMqtt5Client(
	private val preferenceProvider: PreferenceProvider
) {

	private lateinit var mqttAndroidClient: Mqtt5RxClient


	private var recreateDisposables: CompositeDisposable = CompositeDisposable()
	private var subscribeMessageDisposables: CompositeDisposable = CompositeDisposable()
	private val publishMessageDisposables: CompositeDisposable = CompositeDisposable()

	private var preferences: Preferences

	init {
		preferences = preferenceProvider.loadPreferences()
		createMQTTClient()
		subscribeMessagesGlobally()
	}

	fun close(): Completable {
		return mqttAndroidClient.disconnect()
			.doOnComplete {
				Timber.i("Close: disconnected")
				recreateDisposables.clear()
				publishMessageDisposables.clear()
				subscribeMessageDisposables.clear()
			}
			.doOnError {
				Timber.e(it, "Close disconnect: error")
				recreateDisposables.clear()
				publishMessageDisposables.clear()
				subscribeMessageDisposables.clear()
			}
	}

	/**
	 * Führt den Verbindungsaufbau des Clients mit dem Broker aus. Es werden die im [preferenceProvider] hinterlegten
	 * Argumente verwendet.
	 *
	 * [topics] enthält eine Liste von [ITopic]-[QoS] Paaren, für die eine Subscription beim Broker angelegt wird.
	 */
	fun connect(topics: List<Pair<String, Int>>): Completable {
		return mqttAndroidClient.connectWith()
			.cleanStart(preferences.isCleanSession)
			.sessionExpiryInterval(preferences.sessionExpiryInterval)
			.keepAlive(preferences.keepAlive)
			.applyConnect()
			.doOnSuccess {
				Timber.i(
					"HiveMq client connected; clientId=[%s], reasonCode=[%s]",
					preferences.clientId, it.reasonCode.toString()
				)
			}
			.doOnError { ex -> Timber.e(ex, "Could not connect HiveMq client") }
			.ignoreElement()
			.andThen(createSubscriptions(topics))
	}

	private fun createSubscriptions(topics: List<Pair<String, Int>>): Completable {
		return Observable.fromIterable(topics)
			.doOnNext {
				Timber.i("Creating subscription for: topic=[%s], qos=[%s]", it.first, it.second)
			}
			.flatMapCompletable { topicPair ->
				mqttAndroidClient.subscribeStreamWith()
					.topicFilter(topicPair.first)
					.qos(MqttQos.fromCode(topicPair.second)!!)
					.applySubscribe()
					.doOnSingle { result -> Timber.i("HiveMq Subscription successful: subAck=[%s]", result) }
					.doOnError { ex -> Timber.e(ex, "HiveMq Subscription error:") }
					.retryWhen { errors ->
						errors.delay(10, TimeUnit.SECONDS)
							.doOnNext { Timber.i("HiveMq Subscription retry: reason=[${it.localizedMessage}]") }
					}
					.ignoreElements()
			}
	}

	/**
	 * Disconnected den Client vom Broker.
	 */
	fun disconnect(): Completable {
		return mqttAndroidClient.disconnect()
			.doOnComplete { Timber.i("HiveMq client disconnected") }
			.doOnError { ex -> Timber.e(ex, "Could not disconnect HiveMq client") }
	}

	private fun createMQTTClient() {
		mqttAndroidClient = MqttClient.builder()
			.useMqttVersion5()
			.addConnectedListener {
				Timber.i("Connected; clientId=[%s]", mqttAndroidClient.config.clientIdentifier)
			}
			.addDisconnectedListener {
				Timber.i(
					"Disconnected; clientId=[%s], cause=[%s]",
					mqttAndroidClient.config.clientIdentifier,
					it.cause.message
				)

			}
			.identifier(preferences.clientId)
			.serverHost(preferences.serverAdress)
			.serverPort(preferences.port)
			.automaticReconnect()
			.initialDelay(500, TimeUnit.MILLISECONDS)
			.maxDelay(preferences.maxReconnectDelay.toLong(), TimeUnit.SECONDS)
			.applyAutomaticReconnect()
			.buildRx()
	}

	/**
	 * Subscribed auf ALLE Nachrichten die vom Broker an diese ClientId verschickt werden. Dies beinhaltet auch
	 * Nachrichten an Topics die zuvor am Broker subscribed waren. Eingehende Nachrichten werden auf das globale
	 * PublishSubject gepublisht, auf das mit der Methode [subscribeTopic] subscribed werden kann.
	 */
	private fun subscribeMessagesGlobally() {
		val disposable = mqttAndroidClient.publishes(MqttGlobalPublishFilter.ALL)
			.doOnError { ex -> Timber.e(ex, "global subscribe: error") }
			.subscribe(
				{
					Timber.i(
						"global subscribe: clientId=[%s], topic=[%s], payload=[%s], expiry=[%s]",
						mqttAndroidClient.config.clientIdentifier, it.topic, String(it.payloadAsBytes),
						it.messageExpiryInterval
					)
				},
				{
					Timber.e(it, "global subscribe: error")
				}
			)
		subscribeMessageDisposables.add(disposable)
	}


	interface PreferenceProvider {
		fun loadPreferences(): Preferences
	}

	data class Preferences(
		val clientId: String,
		val clientTopicIdentifier: String,
		val topicPrefix: String,
		val user: String,
		val password: String,
		val serverAdress: String,
		val port: Int,
		val isCleanSession: Boolean,
		val keepAlive: Int,
		val maxReconnectDelay: Int,
		val sessionExpiryInterval: Long
	)
}