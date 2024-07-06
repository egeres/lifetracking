import pytest

from lifetracking.Node_files_audios import Reader_audios


def test_node_readervideos_0():
    with pytest.raises(AssertionError):
        Reader_audios("this_dir_does_not/exist")
