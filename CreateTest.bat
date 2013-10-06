rmdir /s /q test1
rmdir /s /q test2 
rmdir /s /q test3

mkdir test1
mkdir test2

echo "Same in both" >test1\samefile
echo "Same in both" >test2\samefile

echo "Crosslink" >test1\crosslink
mklink /H test2\crosslink test1\crosslink

echo "test1 contents" >test1\difffile
echo "test2 contents larger" >test2\difffile

echo "Hardlink" >test1\hardlink1
mklink /H test1\hardlink2 test1\hardlink1

echo "Not hardlink" >test2\hardlink2

mkdir test1\dirinboth
mkdir test2\dirinboth

echo "File in both" >test1\dirinboth\fileinboth
echo "File in both" >test2\dirinboth\fileinboth

mkdir test1\dirinboth\dirinboth
mkdir test2\dirinboth\dirinboth
mkdir test1\dirinboth\dirin1only
echo "file in dir in 1 only" >test1\dirinboth\dirin1only\fileindirin1only
mkdir test2\dirinboth\dirin2only

mkdir test1\dirin1filein2
echo "file in 2" >test2\dirin1filein2
echo "extrafile" >test1\dirin1filein2\extrafile

mkdir test1\dirin1filein2\contentdir
echo "Contentfile" > test1\dirin1filein2\contentfile


mklink /J test1\junc1 test2\dirinboth
mklink /J test2\junc1 test2\dirinboth

mklink /J test1\junc2 test2\dirinboth
mklink /D test2\junc2 test2\dirinboth\dirinboth

mklink /D test1\symlink1 test1\dirin1filein2
mklink test2\symlink1 test2\dirin1filein2


