# Luminex Application


### __Installing luminex__ from PyPi (Latest Release):
```console
pip install luminex
```
or
```console
pip install git+https://github.com/DISHDevEx/luminex-application.git
```

### __Installing luminex__ from local build (beta testing):
1. Navigate into the root _luminex_ directory.
```console
cd luminex-application
```
2. Run the following command to create the wheel file
 
```console
python setup.py bdist_wheel --version <VERSION_NUMBER>
```
**NOTE**: the ***<VERSION_NUMBER>*** only effects your local build.  You can use any version number you like.  This can be helpful in testing prior to submitting a pull request.  Alternatively, you can eclude the ***--version <VERSION_NUMBER>*** flag and the .whl file name will output as ***devex_sdk-_VERSION_PLACEHOLDER_-py3-none-any.whl***

3. Next, pip install the wheel file by running the following command, note that the _version_ will change depending upon the release:
```console
pip install /dist/luminex-<VERSION_NUMBER>-py3-none-any.whl
```
### __Usage__

Once complete, _luminex_ will be available in your Python evironment for use.  Enter your Python environment and import _devex_sdk_ as you would with any other library or package.
```console
>>> import luminex
```
All functions contained in _luminex_ available for use can be listed by listing the package directory structure:
```console
>>> dir(luminex)
```


## __History__
View version history and release notes in [HISTORY](https://github.com/DISHDevEx/luminex-application/blob/main/HISTORY.md). 

## __Contributing__
Learn how about [CONTRIBUTING](https://github.com/DISHDevEx/luminex-application/blob/main/CONTRIBUTING.md) to luminex.

## __Releases on GitHub__
View all [Luminex releases](https://github.com/DISHDevEx/luminex-application/releases) on GitHub.

## __Releases on PyPi__
View all [Luminex release](https://pypi.org/project/luminex-application/#history) history on PyPi.
