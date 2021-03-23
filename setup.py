{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "engaged-track",
   "metadata": {},
   "outputs": [],
   "source": [
    "import setuptools\n",
    "\n",
    "REQUIRED_PACKAGES = ['google-cloud-storage==1.33.0']\n",
    "PACKAGE_NAME = 'mypackage'\n",
    "PACKAGE_VERSION = '0.0.1'\n",
    "\n",
    "setuptools.setup(\n",
    "    name=PACKAGE_NAME,\n",
    "    version=PACKAGE_VERSION,\n",
    "    author='Shashwat Sagar',\n",
    "    author_email='shashwat.sagar@protonmail.com',\n",
    "    url='https://www.python.org/sigs/distutils-sig/',\n",
    "    description='Demo POV',\n",
    "    install_requires=REQUIRED_PACKAGES,\n",
    "    packages=['mypackage'],\n",
    ")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Apache Beam 2.25.0 for Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
