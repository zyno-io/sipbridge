#!/usr/bin/env python3
"""Check built VitePress file/anchor links without fetching external sites."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlparse

root = Path(__file__).resolve().parents[1] / 'docs/.vitepress/dist'

class Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids, self.links = set(), []
    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            self.ids.add(attrs['id'])
        if tag == 'a' and 'href' in attrs:
            self.links.append(attrs['href'])

pages = {}
for path in root.rglob('*.html'):
    page = Page()
    page.feed(path.read_text())
    pages[path.resolve()] = page
if not pages:
    raise SystemExit('Build the documentation before checking links')
failures, checked = [], 0
for path, page in pages.items():
    for href in page.links:
        uri = urlparse(href)
        if uri.scheme or uri.netloc:
            continue
        target = unquote(uri.path)
        if target.startswith('/sipbridge/'):
            dest = root / target[len('/sipbridge/'):]
        elif target.startswith('/'):
            failures.append((str(path.relative_to(root)), href, 'outside Pages base'))
            continue
        else:
            dest = path.parent / target if target else path
        if dest.is_dir():
            dest = dest / 'index.html'
        elif not dest.suffix:
            dest = dest.with_suffix('.html')
        dest = dest.resolve()
        checked += 1
        if not dest.is_relative_to(root.resolve()) or not dest.is_file():
            failures.append((str(path.relative_to(root)), href, 'missing file'))
        elif uri.fragment and dest in pages and unquote(uri.fragment) not in pages[dest].ids:
            failures.append((str(path.relative_to(root)), href, 'missing anchor'))
for failure in failures:
    print(': '.join(failure))
print(f'Checked {len(pages)} pages and {checked} local links; {len(failures)} failures')
if failures:
    raise SystemExit(1)
