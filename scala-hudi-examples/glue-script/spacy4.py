import os
import site
from importlib import reload
import sys
import boto3
''' 
TODO ?? check upfront if installed and not install 
'''
from setuptools.command import easy_install
install_path = os.environ['GLUE_INSTALLATION']
easy_install.main( ["--install-dir", install_path, "spacy==2.3.1"] )
easy_install.main( ["--install-dir", install_path, "spacy-langdetect"] )
reload(site)
 
import spacy
from spacy_langdetect import LanguageDetector
 
def downloadDirectoryFroms3(bucketName, remoteDirectoryName):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucketName)
    for obj in bucket.objects.filter(Prefix = remoteDirectoryName):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key)
''' 
TODO ?? check upfront if already available in tmp and don't download if already exists 
''' 
downloadDirectoryFroms3('aaktests','Glue/model/en_core_web_sm/')
 
nlp = spacy.load(f'/tmp/Glue/model/en_core_web_sm/en_core_web_sm-2.3.1')
nlp.add_pipe(LanguageDetector(), name="language_detector", last=True)
text = "This is English text. Er lebt mit seinen Eltern und seiner Schwester in Berlin. Yo me divierto todos los días en el parque. Je m'appelle Angélica Summer, j'ai 12 ans et je suis canadienne."
doc = nlp(text)
# document level language detection. Think of it like average language of document!
print(doc._.language)
# sentence level language detection
for i, sent in enumerate(doc.sents):
    print(sent, sent._.language)
 
# Token level language detection from version 0.1.2
# Use this with caution because, in some cases language detection will not make sense for individual tokens
for token in doc:
    print(token, token._.language)
