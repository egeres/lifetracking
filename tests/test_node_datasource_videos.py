import pytest

from lifetracking.Node_files_videos import Reader_videos


def test_node_readervideos_0():

    with pytest.raises(ValueError, match="this_dir_does_notexist doesn't exist"):
        Reader_videos("this_dir_does_notexist")
