from distutils.core import setup

setup(
    name='hub',
    version='0.0.1',
    author='Kris Saxton',
    author_email='kris@automationlogic.com',
    package_dir={'': 'src'},
    packages=['hub', 'hub.lib'],
    scripts=['bin/ctrl-hub-dispatcher', 'bin/ctrl-hub-worker', 'bin/getjob.py',
             'bin/hub-client', 'bin/putjob.py', 'bin/putresult.py'],
    url='https://github.com/KrisSaxton/hub',
    license='LICENSE',
    description='Python based orchestration engine',
    long_description=open('README.md').read(),
    install_requires=['pika'],
)
