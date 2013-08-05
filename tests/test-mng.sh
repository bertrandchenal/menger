export PYTHONPATH=../
export PATH=../bin/:$PATH
. ./assert.sh

mng load example.json
assert "echo $?" 0
assert "mng fetch" "total\n65"
assert "mng fetch category=Jewellery" "total\n5"
assert 'mng fetch category="Electronics/Television & Video"' "total\n20"
assert "mng drill category=Electronics" "Electronics/Camera & Photo\nElectronics/Computers\nElectronics/Television & Video"
assert_end
