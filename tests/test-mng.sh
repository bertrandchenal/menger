export PYTHONPATH=../
export PATH=../bin/:$PATH
. ./assert.sh

mng load example.json
assert "echo $?" 0
assert "mng dice total" "total\n65"
assert "mng dice category=Jewellery total" "category\ttotal\nJewellery\t5"
assert 'mng dice category="Electronics/Television & Video" total' "category\ttotal\nElectronics/Television & Video\t20"
assert "mng drill category=Electronics" "Electronics/Camera & Photo\nElectronics/Computers\nElectronics/Television & Video"
assert_end
