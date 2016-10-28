from logging import getLogger
import multiprocessing
from multiprocessing.pool import Pool
import time
import re

from datetime import timedelta
from django.core.management import BaseCommand
from django.core.management import CommandError
from django.utils.timezone import now

from django_bulk_celery_tasks.utils import ProgressbarMixin, str2bool

logger = getLogger(__name__)


class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class NoDaemonPool(Pool):
    Process = NoDaemonProcess


class BaseManagementCommand(BaseCommand, ProgressbarMixin):

    def multiprocess(self, target, args_list, processes=3, extra_args=None, return_imap=False, progress=True):
        """
        :param target: function (IMPORTANT! function must be importable by path, so declared outside of classes)
        :param args_list: iterable
        :param processes: number of parallel processes
        :param extra_args: joins this list of args to main args. target must unpack the arguments manually
        :param return_imap: boolean, if true, IMapUnorderedIterator is returned instead of a list containing results
        :param progress: boolean, whether to show progressbar or not
        :return: an iterable of results given by target (IMapUnorderedIterator or list)
        """
        if extra_args:
            assert isinstance(extra_args, (list, tuple)), 'extra_args must be a list'
            args_list = [[arg] + extra_args for arg in args_list]

        count = len(args_list)

        logger.info("Starting multiprocess Pool: target={}, processes={}, for {} args".format(target, processes, count))
        if progress:
            self.progress_start(count)
        pool = NoDaemonPool(processes=processes)
        imap_result = pool.imap_unordered(target, args_list)

        try:
            while True:
                index = getattr(imap_result, '_index')
                if index == count:
                    break
                if progress:
                    self.progress_set(index + 1)
                time.sleep(0.1)
            if progress:
                self.progress_finish()
        except KeyboardInterrupt as e:
            pool.terminate()
            pool.join()
            raise CommandError('Caught KeyboardInterrupt, Pool workers terminated')
        else:
            pool.close()
            pool.join()

        if return_imap:
            return imap_result
        return [result for result in imap_result]

    @staticmethod
    def build_kwargs(options_list):
        """
        Builds a dict of kwargs froma list of options.
        Example: ["pk__lt=10", "is_active=true", "pk__in=(1,2,3,4)"]
        becomes  {"pk__lt": 10, "is_active": True, "pk__in": [1, 2, 3, 4]}
        :type options_list: list
        :return dict
        """
        if not options_list:
            return {}

        kwargs = {}
        date_pattern = re.compile(r'^date:(?P<sign>[-+]?)(?P<value>\d+)(?P<unit>[dmy])$')
        delta_kwargs_map = {'d': 'days', 'm': 'months', 'y': 'years'}

        for opt in options_list:
            if '=' not in opt:
                raise CommandError('The following option_kwarg has an invalid format '
                                   '(does not have "="): {}'.format(opt))
            key, value = opt.split('=')

            if value.isdigit():
                value = int(value)
            elif value.lower() in ('true', 'false'):
                value = str2bool(value)
            elif value.lower() in ('null', 'none'):
                value = None
            elif value.startswith('date:') and date_pattern.match(value):
                match = date_pattern.match(value)
                data = match.groupdict()
                data['value'] = int(data['value'])

                delta = timedelta(**{delta_kwargs_map[data['unit']]: data['value']})
                if data['sign'] == '-':
                    value = now().date() - delta
                else:
                    value = now().date() + delta

            elif value.startswith('(') and value.endswith(')'):
                value = [int(item) if item.isdigit() else item for item in value[1:-1].split(',')]
            elif value.startswith('[') and value.endswith(']'):
                value = [int(item) if item.isdigit() else item for item in value[1:-1].split(',')]

            kwargs[key] = value

        return kwargs
