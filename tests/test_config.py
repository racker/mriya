from configparser import ConfigParser
from io import BytesIO

options = ['consumer_key', 'consumer_secret', 'user', 'pass', 'host_prefix']
sections = {'src': options, 'dst': options}

def get_text_config_lines():
    res = []
    for section_name in sections:
        res.append('[%s]\n' % section_name)
        for optname in sections[section_name]:
            res.append('%s = %s_%s_abcxyz\n' % \
                       (optname, section_name, optname))
    return res

def get_file_object_config():
    res_fp = BytesIO()
    txt_lines = get_text_config_lines()
    print txt_lines
    res_fp.writelines(txt_lines)
    res_fp.seek(0)
    return res_fp

def test_config_parse():
    # prepare
    config = ConfigParser()
    config.read_file(get_file_object_config())
    # test
    assert len(options)
    assert len(sections)
    for section_name in sections:
        for optname in sections[section_name]:
            conf_value = config.get(section_name, optname)
            expected = '%s_%s_abcxyz' % (section_name, optname)
            assert conf_value == expected
