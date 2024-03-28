from lifetracking.Node_files_audios import Reader_audios


def test_node_readervideos_0():
    a = Reader_audios("this_dir_does_not/exist")
    o = a.run()
    assert o is None
