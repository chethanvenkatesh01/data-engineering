import setuptools

setuptools.setup(
name='connectors',
version='0.1',
install_requires=['pymssql', 'pymysql', 'psycopg2','python-dotenv','paramiko','ssh2-python==1.0.0','google-cloud-storage','gcsfs==2022.8.2'],
packages=setuptools.find_packages(),
)
