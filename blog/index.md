---
layout: default
title: Bolt Blog
---

# Bolt Blog

欢迎来到 Bolt Blog。

Bolt 是一个 C++ 加速库，提供可组合、可扩展且高性能的数据处理工具集。它面向数据分析系统的物理执行层，目标是在 Spark、Flink、Presto、OpenSearch 等计算框架中，以尽量少的业务代码改动提供统一的高性能执行能力。

Bolt 关注多框架、多硬件和多数据源场景下的执行加速能力，支持面向 Parquet、ORC、Text、CSV、Paimon 等存储格式的数据处理，并持续探索在 CPU、DPU、GPU 等不同硬件环境中的性能优化实践。

这个 Blog 会记录 Bolt 的设计思路、开发实践、性能优化经验、生态集成进展和社区动态。

## 文章列表

{% for post in site.posts %}
### [{{ post.title }}]({{ post.url | relative_url }})

{{ post.date | date: "%Y-%m-%d" }}

{{ post.excerpt | strip_html | truncate: 160 }}

{% endfor %}
