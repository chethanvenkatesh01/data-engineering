import setuptools

setuptools.setup(
    name='dataingestion',
    version='0.1',
    install_requires=["apache-beam[gcp]==2.35.0","beam-nuggets==0.18.1","apitools"],
    packages=setuptools.find_packages(),
)
