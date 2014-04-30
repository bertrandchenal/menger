export PYTHONPATH=../
export PATH=../bin/:$PATH
. ./assert.sh

mng load example.json
assert "echo $?" 0
assert "mng dice" "total\n65"
assert "mng dice category=Jewellery" "total\n5"
assert 'mng dice category="Electronics/Television & Video"' "total\n20"
assert "mng drill category=Electronics" "Electronics/Camera & Photo\nElectronics/Computers\nElectronics/Television & Video"
assert_end
