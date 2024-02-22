from distutils.core import setup


setup_info = {'name':'record_matcher',
              'version': '0.0.2',
              'description': 'Finds matches between two sets of tabular records.',
              'author': 'Johanan Tai',
              'author_email': 'jtai.dvlp@gmail.com',
              'url':'https://github.com/jtai-dev/record_matcher',
              'packages':['record_matcher']
              }

setup(**setup_info)