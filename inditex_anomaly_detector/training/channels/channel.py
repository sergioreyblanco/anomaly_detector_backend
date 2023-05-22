

class Channel:


    @property
    def name(self):
        return self._name

    def __init__(self, name, notification_handler, plot_configuration, plot_credentials):
        self._name = name
        self._notification_handler = notification_handler
        self._plot_configuration=plot_configuration
        self._plot_credentials=plot_credentials

    def send(self, params):
        pass

    @property
    def plot_configuration(self):
        return self._plot_configuration

    @property
    def plot_credentials(self):
        return self._plot_credentials

