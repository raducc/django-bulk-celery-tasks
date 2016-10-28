from __future__ import print_function
from celery import chain, group
from django.utils.module_loading import import_string

from django_bulk_celery_tasks.decorators import multiprocess_func
from django_bulk_celery_tasks.management.base import BaseManagementCommand
from django_bulk_celery_tasks.utils import pprint, str2bool


@multiprocess_func
def execute_action(task_arg, tasks, queue, options):
    extra_options = {}
    if queue:
        extra_options['queue'] = queue

    if options['async']:
        if options['chain']:
            chain(*[task().si(task_arg, **options['task_kwargs']) for task in tasks]).apply_async(**extra_options)
        elif options['group']:
            group(*[task().si(task_arg, **options['task_kwargs']) for task in tasks]).apply_async(**extra_options)
        else:
            for task in tasks:
                task().apply_async(args=[task_arg], kwargs=options['task_kwargs'], **extra_options)
    else:
        for task in tasks:
            task().run(task_arg, **options['task_kwargs'])


class Command(BaseManagementCommand):
    def add_arguments(self, parser):
        """
        :type parser: argparse.ArgumentParser
        """
        task_group = parser.add_mutually_exclusive_group(required=True)
        task_group.add_argument('--task', help='Dotted TASK path (middle ".tasks." is optional). Example: '
                                               'stores.tasks.StoreDailyTask or stores.StoreDailyTask')
        task_group.add_argument('--chain', nargs='+', help='Dotted TASKS paths to run in chain(). '
                                                           'Middle .tasks. is optional')
        task_group.add_argument('--group', nargs='+', help='Dotted TASKS paths to run in group(). '
                                                           'Middle .tasks. is optional')

        source_group = parser.add_mutually_exclusive_group(required=True)
        source_group.add_argument('--model', help='Dotted MODEL path (middle ".models." is optional). '
                                                  'Example: stores.models.Store or stores.Store')
        source_group.add_argument('--range', type=int, nargs=2)

        parser.add_argument('--async', default='true', help='Run tasks or add them to queue')
        parser.add_argument('--distinct', default=False, action='store_true', help='Applies .distinct() to queryset')

        parser.add_argument('--queue', required=False, help='Override task\'d defined queue')
        parser.add_argument('--model-manager', default='objects', help='Use different model manager (default: objects)')
        parser.add_argument('--values-list', default=['pk'], nargs=1,
                            help='Parameter to send to task args=[] when applying async')
        parser.add_argument('--filter', nargs='*', help='Keyword arguments for .filter() to apply on queryset.'
                                                        'Example: --filter pk_gt=10 pg__lt=30')
        parser.add_argument('--order-by', nargs='*', default=['pk'], help='Arguments for .order_by() to apply on '
                                                                          'queryset. Default: --order-by pk')
        parser.add_argument('--exclude', nargs='*', help='Keyword arguments for .exclude() to apply on queryset.'
                                                         'Example: --exclude pk__in=[1,2,3]')
        parser.add_argument('--task-kwargs', nargs='*', help='Keyword arguments for task. Same syntax as --filter/excl')
        parser.add_argument('--processes', type=int, default=1,
                            help='Number of parallel processes used for running/adding tasks')
        parser.add_argument('-y', '--yes', action='store_true', help='Skip confirmation prompt and submit tasks')
        parser.add_argument('--skip-count', action='store_true', help='Skip count (useful for huge queries)')

    @staticmethod
    def _import_task(task_path):
        if task_path.count('.') == 1 and '.tasks.' not in task_path:
            task_path = task_path.replace('.', '.tasks.')
        return import_string(task_path)

    def prepare_arguments(self, options):
        tasks = []

        if options['task']:
            tasks = [self._import_task(options['task'])]
        elif options['chain']:
            tasks = [self._import_task(path) for path in options['chain']]
        elif options['group']:
            tasks = [self._import_task(path) for path in options['group']]

        options['tasks'] = tasks
        options['async'] = str2bool(options['async'])
        options['task_kwargs'] = self.build_kwargs(options['task_kwargs'])

        if options['model']:
            if '.models.' not in options['model']:
                options['model'] = options['model'].replace('.', '.models.', 1)

            options['filter'] = self.build_kwargs(options['filter'])
            options['exclude'] = self.build_kwargs(options['exclude'])

            if options['order_by'] and len(options['order_by']) is 1 and not options['order_by'][0].strip():
                options['order_by'] = None

    def handle(self, *args, **options):
        self.prepare_arguments(options)
        tasks = options['tasks']

        processes = options['processes']
        queue = options.get('queue', None)

        print ("Task       : {}".format(pprint(tasks, ret=True, color='CYAN')))
        print ("Queue      : {}".format(pprint(queue, ret=True, color='CYAN')))
        print ("Task kwargs: {}".format(pprint(options['task_kwargs'], ret=True, color='CYAN')))

        if processes > 1:
            print ("Parallel processes: {}\n".format(pprint(processes, ret=True, color='OKBLUE')))

        if options['model']:
            model = import_string(options['model'])
            manager = getattr(model, options['model_manager'])
            qs = manager.all()

            print ("Model      : {}\n".format(pprint(model, ret=True, color='CYAN')))
            if options['filter']:
                print ('QuerySet .filter() kwargs :', pprint(options['filter'], ret=True, color='OKGREEN'))
                qs = qs.filter(**options['filter'])

            if options['exclude']:
                print ('QuerySet .exclude() kwargs:', pprint(options['exclude'], ret=True, color='OKGREEN'))
                qs = qs.exclude(**options['exclude'])

            if options['order_by']:
                print ('QuerySet .order_by() args:', pprint(options['order_by'], ret=True, color='OKGREEN'))
                qs = qs.order_by(*options['order_by'])

            if options['distinct']:
                print ("QuerySet .distinct()")
                qs = qs.distinct()

            print('QuerySet .values_list("{}", flat=True)'.format(*options['values_list']))
            qs = qs.values_list(*options['values_list'], flat=True)

            if not options['skip_count']:
                count = qs.count()
                pprint('QuerySet .count(): {}'.format(count))
            tasks_args = list(qs)
            count = len(tasks_args)

            if options['distinct']:
                tasks_args = list(set(tasks_args))

        else:
            print ('Args range: {} - {}'.format(*options['range']))
            tasks_args = range(options['range'][0], options['range'][1] + 1)
            count = len(tasks_args)

        if count == 0:
            return pprint('\nCannot continue because there are no items in the queryset', color='FAIL')

        try:
            action = 'Add' if options['async'] else 'Run'
            if not options['yes'] and raw_input('\n{} tasks? [y/n]: '.format(action)).lower() != 'y':
                return pprint('\nOperation cancelled', color='FAIL')
        except KeyboardInterrupt:
            return pprint('\n\nOperation cancelled', color='FAIL')

        if processes > 1:
            self.multiprocess(execute_action, tasks_args, extra_args=[tasks, queue, options], processes=processes)
        else:
            self.progress_start(len(tasks_args))
            for task_arg in tasks_args:
                execute_action((task_arg, tasks, queue, options))
                self.progress_step()
            self.progress_finish()
