from lifetracking.Node_files_videos import Reader_videos


def test_node_readervideos_0():
    a = Reader_videos("this_dir_does_not/exist")
    o = a.run()
    assert o is None
