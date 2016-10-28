from __future__ import print_function

import progressbar


class ProgressbarMixin(object):
    pbar = None
    counter = 0
    _progress_bar_enabled = True

    def start_progress(self, maxval):
        if not self._progress_bar_enabled:
            return
        self.counter = 0
        widgets = [
            progressbar.Bar('>'), ' ',
            progressbar.SimpleProgress(), ' - ', progressbar.Percentage(),
            ' - ', progressbar.ETA(), ' / Rate: ', progressbar.FileTransferSpeed(unit=' items'),
            ' ', progressbar.ReverseBar('<')]
        self.pbar = progressbar.ProgressBar(widgets=widgets, maxval=maxval).start()

    def update_progress(self, i):
        if not self._progress_bar_enabled:
            return
        self.pbar.update(i)

    def step_progress(self):
        if not self._progress_bar_enabled:
            return
        self.counter += 1
        self.update_progress(self.counter)

    def finish_progress(self):
        if not self._progress_bar_enabled:
            return
        self.pbar.finish()

    # alias commands (make more sense)
    def progress_start(self, *args, **kwargs):
        return self.start_progress(*args, **kwargs)

    def progress_step(self):
        return self.step_progress()

    def progress_set(self, *args, **kwargs):
        return self.update_progress(*args, **kwargs)

    def progress_finish(self):
        return self.finish_progress()

    def progress_end(self):
        return self.progress_finish()


def str2bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        v = v.lower().strip()
    else:
        v = str(v)
    return v in ("yes", "true", "1", "True")


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    CYAN = '\033[36m'
    LIGHTCYAN = '\033[96m'
    GREY = '\033[90m'


def pprint(text, color=bcolors.WARNING, ret=False):
    if isinstance(color, str):
        color = getattr(bcolors, color, bcolors.WARNING)
    text = "{}{}{}".format(color, text, bcolors.ENDC)
    if ret:
        return text
    print(text)
