<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
<!-- See https://github.com/check-spelling/check-spelling/wiki/Configuration-Examples%3A-advice --> <!-- markdownlint-disable MD033 MD041 -->
<details><summary>If the flagged items do not appear to be text</summary>

If items relate to a ...
* well-formed pattern.

  If you can write a [pattern](https://github.com/check-spelling/check-spelling/wiki/Configuration-Examples:-patterns) that would match it,
  try adding it to the `patterns.txt` file.

  Patterns are Perl 5 Regular Expressions - you can [test](
https://www.regexplanet.com/advanced/perl/) yours before committing to verify it will match your lines.

  Note that patterns can't match multiline strings.

* binary file.

  Please add a file path to the `excludes.txt` file matching the containing file.

  File paths are Perl 5 Regular Expressions - you can [test](
https://www.regexplanet.com/advanced/perl/) yours before committing to verify it will match your files.

  `^` refers to the file's path from the root of the repository, so `^README\.md$` would exclude [README.md](
../tree/HEAD/README.md) (on whichever branch you're using).

</details>
