import os
import re
from setuptools import setup


def find_version(filename):
    _version_re = re.compile(r"__version__ = '(.*)'")
    for line in open(filename):
        version_match = _version_re.match(line)
        if version_match:
            return version_match.group(1)


__version__ = find_version('augpathlib/__init__.py')

with open('README.md', 'rt') as f:
    long_description = f.read()

try:
    if os.name == 'nt':
        raise ImportError('the alternate will fail with a TypeError')
    else:
        import magic
        if hasattr(magic, 'libmagic'):
            magic_dep = "python-magic; os_name != 'nt'"
        else:
            magic_dep = "file_magic; os_name != 'nt'"
except ImportError:
    magic_dep = "python-magic; os_name != 'nt'"

tests_require = ['pytest', 'pytest-runner']
setup(
    name='augpathlib',
    version=__version__,
    description='Augmented pathlib. Everything else you could do with a path.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/tgbugs/augpathlib',
    author='Tom Gillespie',
    author_email='tgbugs@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='pathlib path paths',
    packages=['augpathlib'],
    python_requires='>=3.6',
    tests_require=tests_require,
    install_requires=[
        'gitpython',
        magic_dep,
        'pexpect>=4.7.0',
        #'psutil',
        'python-dateutil',
        "pyxattr; os_name != 'nt'",
        'terminaltables',
        #'Xlib',
    ],
    extras_require={'dev': ['pytest-cov', 'wheel'],
                    'test': tests_require},
    entry_points={
        'console_scripts': [
        ],
    },
)
