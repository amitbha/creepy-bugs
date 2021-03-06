#!/usr/bin/env python3
#coding=utf8
"""
desc: 爬取TED网站中英双语演讲稿，并存为markdown文件
author: amita
version: 1.8
"""

import json
import re
import time
from functools import reduce
from pprint import pprint
from random import choice, randint
from urllib.parse import parse_qs, urljoin, urlparse

#~ import yaml
import requests
from pyquery import PyQuery as pyq
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tenacity import *

# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def validate_filename(name):
    """清除文件名中的非法字符
    name (str): 文件名
    Returns:
        str: 合法文件名
    """
    pattern = r'[\\/:*?"<>|\r\n ]+'
    new_name = re.sub(pattern, "_", name)
    return new_name


class Spider(object):
    def __init__(self, old_urls: str ="history.txt"):
        # awk '/^talk:/{print $2; nextfile}' *.md | sort -b urls.txt - | uniq
        self.session = requests.Session()
        self.cfile = {
            'ua': 'ua.json',
            'header': 'headers.json',
            'proxy': 'proxies.json',
            'hist': old_urls
        }
        self.referer_pattern = {}
        self.ua_pool = {}
        self.headers = {}
        self.proxies = {}
        self.re_meta = re.compile('"__INITIAL_DATA__": (.+)}\)', re.M)
        self.store_path = 'talks/'
        self.urls = set()
        self.hist = None  # 已保存的talkId，防止重复爬取

        self.load_config()

    def __del__(self):
        self.hist.close()

    def load_config(self) -> None:
        with open(self.cfile['ua'], 'r') as u, open(self.cfile['header'], 'r') as h:
            self.ua_pool = json.load(u)
            self.headers = json.load(h)
        with open(self.cfile['proxy'], "r") as p:
            self.proxies = json.load(p)
        self.hist = open(self.cfile['hist'], 'r+')  # 已保存的talkId，防止重复爬取
        for line in self.hist:
            id = line.strip()
            if id: self.urls.add(id)
        else:
            # 检查文件非空，以及是否以换行结尾
            if 'line' in locals() and not line.endswith('\n'):
                self.hist.write('\n')


    # retry_on_result=retry_if_fail
    @retry(
        stop=stop_after_attempt(6),
        wait=wait_random(min=8, max=60),
        # HTTPError('429 Client Error: Rate Limited too many requests. for url: ...'
        retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def request(self, url: str, exp_json: bool =False) -> dict:
        """
        url (str): 网址
        exp_json (bool, optional): 是json请求
        Returns:
            PyQuery or json: 网页内容
        """
        time.sleep(randint(3, 5))  # 道德自觉
        is_retry = self.request.retry.statistics['attempt_number'] > 1
        proxy = self._get_proxy(is_retry)
        r = self.session.get(
            url,
            headers=self.headers[0],
            proxies=proxy,
            verify=False,
            timeout=10)
        r.raise_for_status()  # 一般是触发了网站防护
        # raise requests.exceptions.HTTPError("===test===")
        return r.json() if exp_json else pyq(r.text, parser='html')

    def _get_id_from_url(self, url: str) -> str:
        assert url.strip(), "invalid url!"
        up = urlparse(url, allow_fragments=False).path
        id_by_name = up.split('/')[-1]
        return id_by_name

    def _get_proxy(self, is_retry: bool =False) -> dict:
        if is_retry and self.proxies:
            self.proxies.setdefault('proxy', None)
            proxy = choice(self.proxies['proxy'])
            pprint(f"Trying proxy: {proxy} ...")
            if proxy:
                return {'http': proxy }

    def add_visited_url(self, url: str):
        id_by_name = self._get_id_from_url(url)
        if not id_by_name in self.urls:
            self.urls.add(id_by_name)
            self.hist.writelines([id_by_name, '\n'])

    def is_visited(self, url: str) -> bool:
        id_by_name = self._get_id_from_url(url)
        return id_by_name in self.urls

    def next_talk(self, url: str) -> dict:
        """处理分页，获取所有资源链接
        url (str): 网址
        """
        page = self.request(url)
        cont = page('#browse-results')
        #~ print(cont.html())
        for col in cont.items(".col"):
            msg = col('.media__message')
            link = msg.find('.ga-link')
            href = link.attr('href')
            if self.is_visited(href):
                continue
            meta = {}
            meta["title"] = link.text()
            # msg('.talk-link__speaker').text()
            mt = msg('.meta').find('.meta__val')
            meta['date'] = mt.eq(0).text()
            meta['rated'] = mt.eq(1).text()
            meta["link"] = urljoin(url, href)
            pprint(f'{meta["title"]}, {meta["link"]}')
            yield meta
        paging = cont.find('a.pagination__next')
        if paging:
            href = paging.attr('href')
            q = parse_qs(urlparse(href).query)
            if q and 'page' in q:
                print(f"Page: {q['page'][0]}")
            next_url = urljoin(url, href)
            yield from self.next_talk(next_url)
        else:
            print("The Last Page!!!")

    #~ https://www.ted.com/talks/1766/transcript.json?language=zh-cn
    def get_subtitle(self, id: str) -> dict:
        """下载演讲稿
        id (str): 演讲的id
        Returns:
            dict: 中英双语演讲稿
        """
        subtitle = {}
        for l in ('zh-cn', 'en'):
            subtitle.setdefault(l, [])
        for l, v in subtitle.items():
            url = f'https://www.ted.com/talks/{id}/transcript.json?language={l}'
            subt = self.request(url, True)
            #~ pprint(subt['paragraphs'])
            for p in subt['paragraphs']:
                text = reduce(lambda x, y: f"{x} {y['text']}", p['cues'], '')
                if l == 'zh-cn':
                    text = text.replace("\n", "")
                    text = text.replace(" ", "")
                elif l == 'en':
                    text = text.strip()
                    text = text.replace("\n", " ")
                    text = text.replace("  ", " ")
                v.append(text)
            #~ pprint(v)
        return subtitle

    def _extract_data(self, jsdata: dict) -> dict:
        data = jsdata['talks'][0]
        meta = {
            'title': data['title'],
            'speaker': data['speaker_name'],
            'abstract': data['description'],
            'duration': data['duration'],
            'views': data['viewed_count'],
            'event': data['event'],
            'tags': data['tags'],
            'id': data['id'],
            'talk': data['slug'],
            'comments': 0,
            'who': '',
            'uid': ''
        }
        if 'speakers' in data and data['speakers']:
            meta['who'] = data['speakers'][0]['whotheyare']
            meta['uid'] = data['speakers'][0]['slug']
        time = data['recorded_at']
        meta['date'] = time[:time.find('T')]
        meta['rated'] = ''
        if "comments" in jsdata and jsdata['comments']:
            meta['comments'] = jsdata['comments']['count']
        for t in data['player_talks']:
            if t['id'] == meta['id']:
                meta['pic'] = t['thumb']
                if 'link' not in meta:
                    meta['link'] = t['canonical']
                break
        meta['related_talks'] = []
        for t in data['related_talks']:
            talk = {
                'id': t['id'],
                'talk': t['slug'],
                'speaker': t['speaker'],
                'title': t['title'],
            }
            meta['related_talks'].append(talk)
        return meta

    def get_content(self, url: str):
        """获取资源页内容
        url (str): 网址
        Returns:
            dict: 元数据与内容
        """
        page = self.request(url)
        #~ pprint(page.text())
        mjson = self.re_meta.search(page.text())
        mjson = mjson.group(1).replace("'", "")
        #~ print(mjson)
        mtdata = json.loads(mjson)
        #~ pprint(mtdata['talks'])  # 数据相当全!
        meta = self._extract_data(mtdata)
        #~ pprint(meta)
        subtitle = self.get_subtitle(meta['id'])
        return meta, subtitle

    def output_md(self, meta: dict, data: dict) -> str:
        #~ print(yaml.dump(meta, encoding='utf-8'))
        if meta and data:
            m = meta
            # 元数据
            meta_yaml = ("---", f"title: '{m['title']}'", f"speaker:",
                         f"- {m['speaker']}", f"date: {m['date']}",
                         f"event: {m['event']}", f"abstract: {m['abstract']}",
                         f"duration: {m['duration']}", f"views: {m['views']}",
                         f"tags: {m['tags']}", f"rated: [{m['rated']}]",
                         f"lang: zh-cn", f"id: {m['id']}",
                         f"talk: {m['talk']}", f"link: {m['link']}", "---")
            # 抬头
            content = [
                f"##### {m['date']} - {m['speaker']}",
                f"# [{m['title']}]({m['link']})",
                f"观看数：{m['views']} | 话题：{'  '.join(m['tags'])} | 印象：{m['rated']}",
            ]
            if m['pic']:
                content.append(f"![]({m['pic']})")
            content.append(f"> {m['abstract']}")
            # 正文
            subt_pipe_table = ["|原文|翻译|", "| :----- | :----- |"]
            for zp, ep in zip(*data.values()):
                subt_pipe_table.append(f"| {ep} | {zp} |")
            subt = '\n'.join(subt_pipe_table)
            content.append(subt)
            # 补充
            speaker = f"> [**主讲人**](https://www.ted.com/speakers/{m['uid']})：{m['who']}"
            if meta['related_talks']:
                related = [f"#### 相关演讲："]
                for i, t in enumerate(meta['related_talks']):
                    url = urljoin(m['link'], t['talk'])
                    related.append(
                        f"{i+1}. [{t['title']}]({url}) - {t['speaker']}")
                content.append("\n")
                content.append("\n".join(related))

            meta_text = '\n'.join(meta_yaml)
            text = '\n\n'.join(content)
            #~ print(meta_text, '\n', text)
            ep = lambda x: x.split()[-1]
            # [2013].为生命的终结做好准备.TED2013.md
            name = f"[{ep(m['date'])}].{m['title']}.{m['event']}.md"
            filename = self.store_path + validate_filename(name)
            with open(filename, 'w') as f:
                f.writelines([meta_text, '\n\n', text])
                self.add_visited_url(m['talk'])
            return filename

    def run(self, start_url: str) -> None:
        u = urlparse(start_url)
        #~ pprint(u)
        if u.path.startswith('/talks/'):  # 单个talk
            meta, subtitle = self.get_content(start_url)
            self.output_md(meta, subtitle)
        elif u.path == '/talks':  # 全集
            for m in self.next_talk(start_url):
                url = m['link']
                assert url, f"URL for {m['title']} not found!"
                # continue
                meta, subtitle = self.get_content(url)
                meta.update(m)
                self.output_md(meta, subtitle)
                #break


if __name__ == '__main__':
    url = "https://www.ted.com/talks?language=zh-cn&sort=fascinating"
    # url = "https://www.ted.com/talks/gayle_tzemach_lemmon_meet_the_first_women_to_fight_on_the_front_lines_of_an_american_war?language=zh-cn"
    sp = Spider()
    sp.run(url)
