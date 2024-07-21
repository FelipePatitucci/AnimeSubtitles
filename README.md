Scrapper to get anime subtitles from AnimeTosho website.

> [!NOTE]
> Still under development.

# TODO

- Better logic to find best provider (provider that has subs and has more episodes) (how?).
  Loop each provider and make a hash map "name" -> amount_subs.
  If any of them have the same amount as ep_count, then stop and choose it.
  If none of them have all eps, then choose whoever has the highest amount.

- Change tasks implementation to use .map to speedup.

- Investigate cases where MAL ID exists but it is not found.
  Example: https://animetosho.org/series/ao-no-exorcist-shimane-illuminati-hen.17786
  Same for episodes < 16GB. (Maybe fixed with retry logic.)

- Json_table cleanup.
