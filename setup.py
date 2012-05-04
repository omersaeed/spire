from distutils.core import setup

setup(
    name='spire',
    version='1.0.0a1',
    description='A component framework.',
    author='Jordan McCoy',
    author_email='mccoy.jordan@gmail.com',
    license='BSD',
    url='http://github.com/jordanm/spire',
    packages=[
        'spire',
        'spire.drivers',
        'spire.mesh',
        'spire.schema',
        'spire.wsgi',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
