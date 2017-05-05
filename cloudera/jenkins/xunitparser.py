import math
import unittest
from datetime import timedelta
from xml.etree import ElementTree


def to_timedelta(val):
    if val is None:
        return None

    val = val.replace(",","")
    secs = float(val)
    if math.isnan(secs):
        return None

    return timedelta(seconds=secs)


class TestResult(unittest.TestResult):
    def _exc_info_to_string(self, err, test):
        err = (e for e in err if e)
        return ': '.join(err)


class TestCase(unittest.TestCase):
    TR_CLASS = TestResult
    stdout = None
    stderr = None

    def __init__(self, classname, methodname):
        super(TestCase, self).__init__()
        self.classname = classname
        self.methodname = methodname

    def __str__(self):
        return "%s (%s)" % (self.methodname, self.classname)

    def __repr__(self):
        return "<%s testMethod=%s>" % \
               (self.classname, self.methodname)

    def __hash__(self):
        return hash((type(self), self.classname, self.methodname))

    def id(self):
        return "%s.%s" % (self.classname, self.methodname)

    def seed(self, result, typename=None, message=None, trace=None):
        """ Provide the expected result """
        self.result, self.typename, self.message, self.trace = result, typename, message, trace

    def run(self, tr=None):
        """ Fake run() that produces the seeded result """
        tr = tr or self.TR_CLASS()

        tr.startTest(self)
        if self.result == 'success':
            tr.addSuccess(self)
        elif self.result == 'skipped':
            tr.addSkip(self, '%s: %s' % (self.typename, self._textMessage()))
        elif self.result == 'error':
            tr.addError(self, (self.typename, self._textMessage()))
        elif self.result == 'failure':
            tr.addFailure(self, (self.typename, self._textMessage()))
        tr.stopTest(self)

        return tr

    def _textMessage(self):
        msg = (e for e in (self.message, self.trace) if e)
        return '\n\n'.join(msg) or None

    @property
    def alltext(self):
        err = (e for e in (self.typename, self.message) if e)
        err = ': '.join(err)
        txt = (e for e in (err, self.trace) if e)
        return '\n\n'.join(txt) or None

    def setUp(self):
        """ Dummy method so __init__ does not fail """
        pass

    def tearDown(self):
        """ Dummy method so __init__ does not fail """
        pass

    def runTest(self):
        """ Dummy method so __init__ does not fail """
        self.run()

    @property
    def basename(self):
        return self.classname.rpartition('.')[2]

    @property
    def success(self):
        return self.result == 'success'

    @property
    def skipped(self):
        return self.result == 'skipped'

    @property
    def failed(self):
        return self.result == 'failure'

    @property
    def errored(self):
        return self.result == 'error'

    @property
    def good(self):
        return self.skipped or self.success

    @property
    def bad(self):
        return not self.good

    @property
    def stdall(self):
        """ All system output """
        return '\n'.join([out for out in (self.stdout, self.stderr) if out])


class TestSuite(unittest.TestSuite):
    def __init__(self, *args, **kwargs):
        super(TestSuite, self).__init__(*args, **kwargs)
        self.properties = {}
        self.stdout = None
        self.stderr = None


class Parser(object):
    TC_CLASS = TestCase
    TS_CLASS = TestSuite
    TR_CLASS = TestResult

    def parse(self, source):
        xml = ElementTree.parse(source)
        root = xml.getroot()
        return self.parse_root(root)

    def parse_root(self, root):
        ts = self.TS_CLASS()
        if root.tag == 'testsuites':
            for subroot in root:
                self.parse_testsuite(subroot, ts)
        else:
            self.parse_testsuite(root, ts)

        tr = ts.run(self.TR_CLASS())

        tr.time = to_timedelta(root.attrib.get('time'))

        # check totals if they are in the root XML element
        if 'errors' in root.attrib:
            assert len(tr.errors) == int(root.attrib['errors'])
        if 'failures' in root.attrib:
            assert len(tr.failures) == int(root.attrib['failures'])
        if 'skip' in root.attrib:
            assert len(tr.skipped) == int(root.attrib['skip'])
        if 'tests' in root.attrib:
            assert len(list(ts)) == int(root.attrib['tests'])

        return (ts, tr)

    def parse_testsuite(self, root, ts):
        assert root.tag == 'testsuite'
        ts.name = root.attrib.get('name')
        ts.package = root.attrib.get('package')
        for el in root:
            if el.tag == 'testcase':
                self.parse_testcase(el, ts)
            if el.tag == 'properties':
                self.parse_properties(el, ts)
            if el.tag == 'system-out' and el.text:
                ts.stdout = el.text.strip()
            if el.tag == 'system-err' and el.text:
                ts.stderr = el.text.strip()

    def parse_testcase(self, el, ts):
        tc_classname = el.attrib.get('classname') or ts.name
        tc = self.TC_CLASS(tc_classname, el.attrib['name'])
        tc.seed('success', trace=el.text or None)
        tc.time = to_timedelta(el.attrib.get('time'))
        message = None
        text = None
        for e in el:
            # error takes over failure in JUnit 4
            if e.tag in ('failure', 'error', 'skipped'):
                tc = self.TC_CLASS(tc_classname, el.attrib['name'])
                result = e.tag
                typename = e.attrib.get('type')

                # reuse old if empty
                message = e.attrib.get('message') or message
                text = e.text or text

                tc.seed(result, typename, message, text)
                tc.time = to_timedelta(el.attrib.get('time'))
            if e.tag == 'system-out' and e.text:
                tc.stdout = e.text.strip()
            if e.tag == 'system-err' and e.text:
                tc.stderr = e.text.strip()

        # add either the original "success" tc or a tc created by elements
        ts.addTest(tc)

    def parse_properties(self, el, ts):
        for e in el:
            if e.tag == 'property':
                assert e.attrib['name'] not in ts.properties
                ts.properties[e.attrib['name']] = e.attrib['value']


def parse(source):
    return Parser().parse(source)
