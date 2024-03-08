from setuptools import find_packages, setup

setup(
    name="fa-essentials",
    packages=find_packages(
        include=[
            "faessentials",
            # 'faessentials.models',
            # 'faessentials.models.market',
            # 'faessentials.models.exchange',
            # 'faessentials.models.db_independent'
        ],
        exclude=["tests*"],
    ),
    # packages=['faessentials'],
    # package_dir={'faessentials':'src'}
    # version='0.1.0',
    # description='The library describes the most common models used in trading systems.',
    # author='Brayan Svan',
    # license='MIT',
    # install_requires=['sqlmodel'],
    # setup_requires=['pytest-runner'],
    # tests_require=['pytest==4.4.1'],
    # test_suite='tests',
    # cmdclass={
    #     'build': ProduceAvroSchemas,
    # },
)
