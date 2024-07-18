Scrapper to get anime subtitles from AnimeTosho website.

> [!NOTE]
> Still under development.

# TODO

- Better logic to find best provider (provider that has subs and has more episodes) (how?).
  Loop each provider and make a hash map "name" -> amount_subs.
  If any of them have the same amount as ep_count, then stop and choose it.
  If none of them have all eps, then choose whoever has the highest amount.
- Try to remove duplicate links using prefix of link str (should be the same).
- Do not insert info when too many rows (~4k is avg for 12 episodes).
  maybe hard cap at num_ep x 600.
- Remove old tables that are not indexed in json_table.
- Do not merge quotes when name is 'NTP'
