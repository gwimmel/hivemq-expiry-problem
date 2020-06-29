package com.example.mqtttestapplication

import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import timber.log.Timber


class MainActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        Timber.plant(Timber.DebugTree())

        val mqttClient = HiveMqMqtt5Client(MyMqttPreferenceProvider())
        mqttClient.connect(listOf(
            Pair("testtopic", 2)))
            .subscribe {
                Timber.d("MQTT Connect finished")
            }

    }

    class MyMqttPreferenceProvider() : HiveMqMqtt5Client.PreferenceProvider {
        override fun loadPreferences(): HiveMqMqtt5Client.Preferences {
            return HiveMqMqtt5Client.Preferences(
                clientId = "test",
                clientTopicIdentifier = "",
                isCleanSession = false,
                keepAlive = 60,
                maxReconnectDelay = 5,
                password = "",
                port = 1883,
                serverAdress = "192.168.178.23",
                topicPrefix = "",
                user = "",
                sessionExpiryInterval = 3600
            )
        }
    }
}
