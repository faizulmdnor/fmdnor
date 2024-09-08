import requests
from bs4 import BeautifulSoup

wiki_url = 'https://en.wikipedia.org/wiki/List_of_treaties'
response = requests.get(wiki_url)
soup = BeautifulSoup(response.text, 'html.parser')

