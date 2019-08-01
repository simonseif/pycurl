import io
import os
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import TestCase

from requests.exceptions import Timeout, HTTPError, ConnectionError, RequestException, SSLError

from pycurl import download, download_all, _dispatch, _is_valid_url


class TestURLValidation(TestCase):
    def test_invalid_urls(self):
        # maybe test this against the full set of strings from
        # https://github.com/minimaxir/big-list-of-naughty-strings/blob/master/blns.txt
        invalid_urls = [
            r'http:\\foo:8080\some',
            '🐵 🙈 🙉 🙊',
            '̗̺͖̹̯͓Ṯ̤͍̥͇͈h̲́e͏͓̼̗̙̼̣͔ ͇̜̱̠͓͍ͅN͕͠e̗̱z̘̝̜̺͙p̤̺̹͍̯͚e̠̻̠͜r̨̤͍̺̖͔̖̖d̠̟̭̬̝͟i̦͖̩͓͔̤a̠̗̬͉̙n͚͜ ̻̞̰͚ͅh̵͉i̳̞v̢͇ḙ͎͟-҉̭̩̼͔m̤̭̫i͕͇̝̦n̗͙ḍ̟ ̯̲͕͞ǫ̟̯̰̲͙̻̝f ̪̰̰̗̖̭̘͘c̦͍̲̞͍̩̙ḥ͚a̮͎̟̙͜ơ̩̹͎s̤.̝̝ ҉Z̡̖̜͖̰̣͉̜a͖̰͙̬͡l̲̫̳͍̩g̡̟̼̱͚̞̬ͅo̗͜.̟',
            '',
            '\n',
            '://localhost:21/some/file',
            'localhost:21/some/file',
        ]
        for url in invalid_urls:
            self.assertFalse(_is_valid_url(url), msg=f"URL '{url}' accepted")

    def test_valid_urls(self):
        invalid_urls = [
            'http://foo:8080/bar',
            'http://foo/bar',
            'https://foo/bar',
            'https://foo/bar/',
            'http://foo:8080/El Niño/',
        ]
        for url in invalid_urls:
            self.assertTrue(_is_valid_url(url), msg=f"URL '{url}' not accepted")


class TestSSL(TestCase):
    bad_urls = [
        'https://expired.badssl.com/',
        'https://wrong.host.badssl.com/',
        'https://self-signed.badssl.com/',
        'https://untrusted-root.badssl.com/',
    ]

    def test_verify_raises(self):
        for url in self.bad_urls:
            with self.subTest(i=url):
                buffer = io.BytesIO()
                with self.assertRaises(SSLError):
                    download(url, buffer, 10, True)

    def test_not_verify_ok(self):
        for url in self.bad_urls:
            with self.subTest(i=url):
                buffer = io.BytesIO()
                download(url, buffer, 10, False)


class TestDownload(TestCase):
    @classmethod
    def setUpClass(cls):
        server_address = ('127.0.0.1', 0)
        httpd = HTTPServer(server_address, TestServer)
        cls.httpd = httpd
        cls.httpd_thread = threading.Thread(target=httpd.serve_forever)
        cls.httpd_thread.start()
        cls.endpoint = f'http://{httpd.server_address[0]}:{httpd.server_address[1]}'

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd_thread.join()

    def test_simple_download_ok(self):
        num_bytes = 1024
        buffer = io.BytesIO()
        download(f'{self.endpoint}/content/{num_bytes}', buffer, 10, False)
        # read content and assert it contains exactly num_bytes
        self.assertEqual(num_bytes, len(buffer.getbuffer()))

    def test_read_timeout_error(self):
        buffer = io.BytesIO()
        with self.assertRaises(Timeout):
            download(f'{self.endpoint}/sleep/{2}', buffer, 1, False)
        # nothing downloaded
        self.assertEqual(0, len(buffer.getbuffer()))

    def test_http_error(self):
        for error_code in range(400, 600):  # TODO: this range is excessive
            with self.subTest(i=error_code):
                buffer = io.BytesIO()
                with self.assertRaises(HTTPError):
                    download(f'{self.endpoint}/error/{error_code}', buffer, 10, False)
                # nothing downloaded
                self.assertEqual(0, len(buffer.getbuffer()))

    def test_connection_error(self):
        buffer = io.BytesIO()
        with self.assertRaises(ConnectionError):
            download(f'http://localhost:1/something', buffer, 10, False)
        # nothing downloaded
        self.assertEqual(0, len(buffer.getbuffer()))

    def test_illegal_url_error(self):
        illegal_urls = [
            ('wrong scheme', 'ftp://localhost:21/some/file'),
            ('empty url', ''),
            ('newline', '\n'),
            ('whitespace', '\t '),
            ('emojus', '🐵 🙈 🙉 🙊'),
            ('missing protocol', 'localhost:21/some/file'),
        ]
        for (desc, url) in illegal_urls:
            with self.subTest(i=desc):
                buffer = io.BytesIO()
                with self.assertRaises(RequestException):
                    download(url, buffer, 10, False)


class TestDownloadAll(TestCase):
    def test_identical_url_is_not_downloaded_twice(self):
        class Downloader(object):
            def __init__(self):
                self.invocations = 0

            def download(self, url, stream):
                self.invocations += 1
                stream.write(b'foobar')

        downloader = Downloader()
        urls = ['a', 'a']
        with tempfile.TemporaryDirectory() as d:
            download_all(urls, d, downloader.download)
        self.assertEqual(1, downloader.invocations)

    def test_aborted_downloads_are_removed_from_disk(self):
        def broken_downloader(_, stream):
            stream.write(b'foobar')
            raise IOError('broken download')

        urls = ['a', 'b']
        with tempfile.TemporaryDirectory() as d:
            download_all(urls, d, broken_downloader)
            self.assertEqual([], os.listdir(d))

    def test_continue_on_error(self):
        class Downloader(object):
            def __init__(self):
                self.invocations = 0

            def download(self, url, stream):
                self.invocations += 1
                if self.invocations == 1:
                    raise IOError('broken download')
                else:
                    stream.write(b'foobar')

        downloader = Downloader()
        urls = ['a', 'b']
        with tempfile.TemporaryDirectory() as d:
            download_all(urls, d, downloader.download)
            # called twice
            self.assertEqual(2, downloader.invocations)
            # just one download succeeded
            self.assertEqual(1, len(os.listdir(d)))


class TestDispatcher(TestCase):

    def noop(jobs):
        list(jobs)

    def test_no_deadlock_when_no_jobs(self):
        _dispatch([], TestDispatcher.noop, 1)

    def test_no_deadlock_when_more_consumers_than_urls(self):
        _dispatch(['a', 'b', 'c'], TestDispatcher.noop, 10)

    def test_worker_threads_terminated(self):
        num_threads_before = len(threading.enumerate())
        _dispatch(['a', 'b', 'c'], TestDispatcher.noop, 10)
        num_threads_after = len(threading.enumerate())
        self.assertEqual(num_threads_before, num_threads_after)


class TestServer(BaseHTTPRequestHandler):
    """
    TestServer that exposes 3 endpoints:
    /error/<num> returns an error with code <num>
    /content/<num> returns 200 with <num> bytes of content
    /sleep/<num> waits <num> seconds before it returns a 200 response
    """

    def log_message(self, *args):
        pass  # BaseHTTPRequestHandler spams to stderr

    def do_GET(self):
        path_segments = self.path.split('/')  # '/foo/bar' -> ['', 'foo', 'bar'], '/' -> ['', '']
        handler_index = path_segments[1]
        handler_map = {
            'error': self.error_handler,
            'content': self.content_handler,
            'sleep': self.sleep_handler,
        }
        handler = handler_map.get(handler_index, self.not_found_handler)
        handler(*path_segments[2:])

    def error_handler(self, code: str, *ignored):
        self.send_error(int(code))

    def content_handler(self, num_bytes: str, *ignored):
        self.send_response(200)
        self.send_header('Content-Length', num_bytes)
        self.end_headers()
        self.wfile.write(bytes([0x00 for _ in range(int(num_bytes))]))

    def not_found_handler(self, *ignored):
        self.send_error(404)

    def sleep_handler(self, sleep_seconds: str, *ignored):
        time.sleep(int(sleep_seconds))
