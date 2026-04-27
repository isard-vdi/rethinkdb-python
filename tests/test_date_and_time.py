from unittest.mock import ANY, Mock, call, patch

import pytest

from rethinkdb import ast, r


@pytest.mark.unit
class TestNow(object):
    def setup_method(self):
        pass

    def test_get_now(self):
        now = r.now()
        assert type(now) == ast.Now
