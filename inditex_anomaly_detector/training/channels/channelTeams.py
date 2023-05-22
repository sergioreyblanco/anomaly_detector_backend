
from inditex_anomaly_detector.training.channels.channel import Channel
from zcomcule_ml_commons.notifications import TeamsNotifications
from inditex_anomaly_detector.utils.logging_debugging import common_print


class TeamsChannel(Channel):

    def __init__(self, name, config, plot_configuration, plot_credentials):
        super().__init__(name, TeamsNotifications(link=config['URL']), plot_configuration, plot_credentials)
        self._url = config['URL']

    def send(self, params):
        common_print("", self._notification_handler.send(params['message']))
        pass