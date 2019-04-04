export VERSION="0.1.0"
export BUILD_NAME="0"
export CONDA_BLD_PATH=~/conda-bld
PLATFORM="Linux-64"
PKG="esgfpub"

if [ -d $CONDA_BLD_PATH ]; then
    rm -rf $CONDA_BLD_PATH
fi
echo "Creating build dir at" $CONDA_BLD_PATH
mkdir $CONDA_BLD_PATH

conda config --set anaconda_upload no
if [ ! -z "$1" ]; then
    export TAG="$1"
else
    export TAG="master"
fi
echo "Building" $VERSION"-"$BUILD_NAME "for label:" $TAG

conda build -c conda-forge -c e3sm -c bioconda .

if [ $? -eq 1 ]; then
    echo "conda build failed"
    exit
fi

if [ ! -z "$1" ]; then
    anaconda upload -u e3sm -l "$1" $CONDA_BLD_PATH/$PLATFORM/$PKG-$VERSION-$BUILD_NAME.tar.bz2 
else
    anaconda upload -u e3sm $CONDA_BLD_PATH/$PLATFORM/$PKG-$VERSION-$BUILD_NAME.tar.bz2
fi