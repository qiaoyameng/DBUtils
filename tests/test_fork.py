"""Test that PooledDB and PersistentDB are safe against forking.

When a process using a connection pool is forked, the child process
inherits the pool state, including connections and locks.  The child
must not use the inherited connections (they belong to the parent)
and must not block on inherited locks (they may be held by threads
that do not exist in the child).  Instead, it must start over with
a fresh pool state and create its own connections.

These tests use a mock DB-API 2 driver that records every connection
it creates and every critical operation, together with the id of the
process and the number of the connection, in a log file.  This allows
verifying which process used which connection and that the child
process did not touch the connections of the parent process.
"""

import os
import threading
import time
import traceback

import pytest

from dbutils.persistent_db import PersistentDB
from dbutils.pooled_db import InvalidConnectionError, PooledDB

pytestmark = pytest.mark.skipif(
    not hasattr(os, 'fork'), reason='need a platform with fork support')


class MockCursor:
    """Mock DB-API 2 cursor recording executed SQL statements."""

    def __init__(self, con):
        self._con = con

    def execute(self, sql):
        self._con.record(f'execute:{sql}')

    def close(self):
        pass


class MockConnection:
    """Mock DB-API 2 connection recording all critical operations."""

    def __init__(self, log_file, number):
        self.log_file = log_file
        self.number = number
        self.record('connect')

    def record(self, message):
        """Record an operation, prefixed with pid and connection number."""
        with open(self.log_file, 'a', encoding='ascii') as log:
            log.write(f'{os.getpid()}:{self.number}:{message}\n')

    def cursor(self):
        return MockCursor(self)

    def commit(self):
        self.record('commit')

    def rollback(self):
        self.record('rollback')

    def close(self):
        self.record('close')

    def ping(self, reconnect=True):  # noqa: ARG002
        return True


class MockDBModule:
    """Mock DB-API 2 module creating numbered, recording connections."""

    threadsafety = 2

    class Error(Exception):
        pass

    class DatabaseError(Error):
        pass

    class OperationalError(DatabaseError):
        pass

    class InterfaceError(DatabaseError):
        pass

    class InternalError(DatabaseError):
        pass

    def __init__(self, log_file):
        self.log_file = log_file
        self.counter = 0

    def connect(self):
        self.counter += 1
        return MockConnection(self.log_file, self.counter)


@pytest.fixture
def log_file(tmp_path):
    """Provide the path of the log file used by the mock driver."""
    return str(tmp_path / 'mock_db.log')


def record(log_file, message):
    """Record a test message, prefixed with the current pid."""
    with open(log_file, 'a', encoding='ascii') as log:
        log.write(f'{os.getpid()}:test:{message}\n')


def read_records(log_file):
    """Read the log file as a list of (pid, number, message) tuples."""
    records = []
    with open(log_file, encoding='ascii') as log:
        for line in log:
            pid, number, message = line.rstrip('\n').split(':', 2)
            records.append((int(pid), number, message))
    return records


def fork_and_run(func, timeout=15):
    """Run the given function in a forked child process.

    Returns the pid of the child process.  Raises an error if the
    child process fails or does not finish within the timeout.
    """
    pid = os.fork()
    if pid == 0:  # child process
        status = 0
        try:
            func()
        except BaseException:  # noqa: BLE001
            traceback.print_exc()
            status = 1
        finally:
            os._exit(status)
    deadline = time.monotonic() + timeout
    while True:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done == pid:
            break
        if time.monotonic() > deadline:
            os.kill(pid, 9)
            os.waitpid(pid, 0)
            raise TimeoutError('child process did not finish in time')
        time.sleep(0.02)
    assert os.WIFEXITED(status), 'child process did not exit normally'
    assert os.WEXITSTATUS(status) == 0, 'child process failed'
    return pid


def test_pooled_db_fork(log_file):
    """Check that a forked child process gets its own pooled connections."""
    creator = MockDBModule(log_file)
    pool = PooledDB(creator, mincached=1, maxcached=2,
                    setsession=['set datestyle to german'])
    db = pool.connection()
    parent_con = db._con._con
    assert parent_con.number == 1

    def in_child():
        # a connection checked out by the parent must be rejected
        try:
            db.cursor()
        except InvalidConnectionError:
            record(log_file, 'parent proxy rejected')
        else:
            record(log_file, 'parent proxy not rejected')
        # closing it must not return it to the pool or touch it
        db.close()
        # the child must get its own, fresh connection instead
        child_db = pool.connection()
        child_con = child_db._con._con
        record(log_file, f'child got connection {child_con.number}')
        child_db.close()
        pool.close()

    child_pid = fork_and_run(in_child)

    records = read_records(log_file)
    child_records = [r for r in records if r[0] == child_pid]
    # the child created its own connection with the original creator
    child_connects = [r for r in child_records if r[2] == 'connect']
    assert len(child_connects) == 1
    child_number = child_connects[0][1]
    assert child_number != str(parent_con.number)
    # the session was prepared with the original setsession commands
    assert (child_pid, child_number,
            'execute:set datestyle to german') in child_records
    # the old proxy was rejected in the child process
    assert (child_pid, 'test', 'parent proxy rejected') in child_records
    # the child got a different connection than the parent
    assert (child_pid, 'test',
            f'child got connection {child_number}') in child_records
    # the child did not roll back or close the parent's connection
    parent_con_records = [
        r for r in child_records if r[1] == str(parent_con.number)]
    assert parent_con_records == []
    # the parent's pool is still intact and hands out the same connection
    db.close()
    db = pool.connection()
    assert db._con._con.number == parent_con.number
    db.close()
    pool.close()


def test_pooled_db_shared_fork(log_file):
    """Check fork safety of PooledDB with shared connections."""
    creator = MockDBModule(log_file)
    pool = PooledDB(creator, mincached=1, maxcached=2, maxshared=2)
    db = pool.connection()
    parent_con = db._con._con
    assert parent_con.number == 1

    def in_child():
        try:
            db.cursor()
        except InvalidConnectionError:
            record(log_file, 'parent proxy rejected')
        else:
            record(log_file, 'parent proxy not rejected')
        db.close()
        child_db = pool.connection()
        record(log_file, f'child got connection {child_db._con._con.number}')
        child_db.close()
        pool.close()

    child_pid = fork_and_run(in_child)

    records = read_records(log_file)
    child_records = [r for r in records if r[0] == child_pid]
    child_connects = [r for r in child_records if r[2] == 'connect']
    assert len(child_connects) == 1
    assert child_connects[0][1] != str(parent_con.number)
    assert (child_pid, 'test', 'parent proxy rejected') in child_records
    parent_con_records = [
        r for r in child_records if r[1] == str(parent_con.number)]
    assert parent_con_records == []
    db.close()
    pool.close()


def test_persistent_db_fork(log_file):
    """Check that a forked child process gets its own persistent connection."""
    creator = MockDBModule(log_file)
    persist = PersistentDB(creator, setsession=['set time zone utc'])
    db = persist.connection()
    parent_con = db._con
    assert parent_con.number == 1

    def in_child():
        child_db = persist.connection()
        record(log_file, f'child got connection {child_db._con.number}')
        # the child connection is thread-affine as usual
        assert persist.connection() is child_db

    child_pid = fork_and_run(in_child)

    records = read_records(log_file)
    child_records = [r for r in records if r[0] == child_pid]
    # the child created its own connection with the original creator
    child_connects = [r for r in child_records if r[2] == 'connect']
    assert len(child_connects) == 1
    child_number = child_connects[0][1]
    assert child_number != str(parent_con.number)
    # the session was prepared with the original setsession commands
    assert (child_pid, child_number,
            'execute:set time zone utc') in child_records
    assert (child_pid, 'test',
            f'child got connection {child_number}') in child_records
    # the child did not roll back or close the parent's connection
    parent_con_records = [
        r for r in child_records if r[1] == str(parent_con.number)]
    assert parent_con_records == []
    # the parent still uses its original connection
    assert persist.connection() is db


# forking a multi-threaded process is intentional in this test
@pytest.mark.filterwarnings(
    'ignore:This process .* is multi-threaded.*:DeprecationWarning')
def test_pooled_db_fork_with_held_lock(log_file):
    """Check that a child does not block on an inherited held pool lock."""
    creator = MockDBModule(log_file)
    pool = PooledDB(creator, mincached=1)
    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def hold_lock():
        pool._lock.acquire()
        lock_acquired.set()
        release_lock.wait(30)
        pool._lock.release()

    thread = threading.Thread(target=hold_lock)
    thread.start()
    assert lock_acquired.wait(10)
    try:
        def in_child():
            # the inherited lock is held by a thread that does not exist
            # in the child, so the pool must not wait for it, but start
            # over with a fresh lock and a fresh connection
            child_db = pool.connection()
            record(log_file,
                   f'child got connection {child_db._con._con.number}')
            child_db.close()

        child_pid = fork_and_run(in_child)
    finally:
        release_lock.set()
        thread.join(10)

    records = read_records(log_file)
    child_records = [r for r in records if r[0] == child_pid]
    child_connects = [r for r in child_records if r[2] == 'connect']
    assert len(child_connects) == 1
    assert (child_pid, 'test',
            f'child got connection {child_connects[0][1]}') in child_records
    # the parent's pool is still intact
    db = pool.connection()
    assert db._con._con.number == 1
    db.close()
    pool.close()
