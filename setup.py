from setuptools import setup


setup(
    name='django-bulk-celery-tasks',
    description='Build a list of arguments (from queryset) and submit a batch of tasks for those arguments',
    keywords='django, model, queryset, update, bulk, celery, task',
    author='Radu Cancel',
    license='MIT License',
    author_email='radu.cancel@gmail.com',
    maintainer='Simion Baws',
    version='0.1',
    maintainer_email='simion.agv@gmail.com',
    url='https://github.com/simion/django-bulk-celery-tasks/',

    packages=['django_bulk_update'],
    install_requires=['django >=1.8', 'progressbar', 'celery'],

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
    ],
)
