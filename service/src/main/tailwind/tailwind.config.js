/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Tailwind config for the Hive WebUI.
// Build (run from the repo root, with the standalone tailwindcss CLI). The CSS
// is committed and served locally by HiveServer2's Jetty (no CDN, airgap-safe);
// the final perl step strips Tailwind's attribution banner so the artifact has
// no external URL strings at all:
//   tailwindcss -c service/src/main/tailwind/tailwind.config.js \
//     -i service/src/main/tailwind/input.css \
//     -o service/src/resources/hive-webapps/static/css/hive.tw.css --minify \
//   && perl -i -pe 's{/\*!.*?\*/}{}g' service/src/resources/hive-webapps/static/css/hive.tw.css
module.exports = {
  content: [
    "service/src/resources/hive-webapps/**/*.{jsp,jspf,html,js}",
    "service/src/jamon/**/*.jamon",
    "service/src/java/org/apache/hive/service/servlet/WebUiLayout.java",
  ],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        brand: {
          DEFAULT: "#f2b134",
          50: "#fef6e7",
          100: "#fde9c2",
          400: "#f2b134",
          500: "#e09e22",
          600: "#c4861a",
          700: "#9c6a16",
        },
      },
      fontFamily: {
        sans: ["system-ui", "-apple-system", "Segoe UI", "Roboto", "Helvetica", "Arial", "sans-serif"],
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "Consolas", "monospace"],
      },
    },
  },
  plugins: [],
};
