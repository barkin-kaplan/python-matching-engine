from helper.notification.i_notifier import INotifier


class ConsoleNotifier(INotifier):
    def notify(self, message: str):
        print(f"Notification : {message}")
