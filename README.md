# creepy-bugs
羽量级爬虫项目

##### 一、何谓羽量？

- 刚好能用
- 简洁、易扩展
- 单文件

所谓“麻雀虽小，五脏俱全”是也！

##### 二、爬什么？

1. 网站资源：如TED所有演讲稿
   - 列表页：包含了所有资源的链接，一般带分页。
   - 资源页：文本、图片、格式、音视频等。
   - 扩展内容：参考文献、作者信息等。
   - **思路**：
     - 广度优先：优先爬取所有资源链接
     - 深度优先：优先爬取资源
     - 爬虫的道德规范，与反爬措施
2. 网页资源：如百度文库、优酷视频

##### 三、项目

1. TED 演讲
   爬取中英双语演讲稿，存为markdown格式。格式为：
   - 元数据：标题、时间、作者、摘要、分类、id等，方便后期整理
   - 正文：标题、题图、中文段-英文段
   - 扩展信息：无