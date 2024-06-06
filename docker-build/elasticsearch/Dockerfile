ARG ELK_VERSION

FROM elasticsearch:${ELK_VERSION}

# Add your elasticsearch plugins setup here
# Example: RUN elasticsearch-plugin install analysis-icu
#RUN elasticsearch-plugin install x-pack

RUN elasticsearch-plugin install https://bintray.com/evo/elasticsearch/download_file?file_path=analysis-jmorphy2-0.2.2-es7.9.3.zip