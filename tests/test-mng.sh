export PYTHONPATH=../
export PATH=../bin/:$PATH
. ./assert.sh

mng load example.json
assert "echo $?" 0
assert "mng fetch" "total\n65.0"
assert "mng fetch category=Jewellery" "category\ttotal\nJewellery\t5.0"
assert 'mng fetch category="Electronics/Television & Video"' "category\ttotal\nElectronics/Television & Video\t20.0"
assert "mng drill category=Electronics" "Electronics/Television & Video\nElectronics/Computers\nElectronics/Camera & Photo"
assert_end
