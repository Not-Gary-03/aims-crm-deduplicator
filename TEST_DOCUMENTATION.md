# IMPORTANT NOTES
TO RUN IN TERMINAL: "python deduplicate_leads.py"

PROBLEMS TO SOLVE:
- How automated does this need to be? Fully automated duplicate detection might be faulty, so relying on it to remove duplicates without involving any real people might cause problems.
- If we need humans to audit any deduplications, who will be doing this and how long will it take?
- How do we identify duplicates? Different leads can have wildly differing data but hypothetically still represent the same person. More nuance and complexity may be required in this process. Ask around.

# TESTING DOCUMENTATION
## TEST 1 2026/04/08
Unmodified Claude-written program. Uses three dupe checks: email exact, phone number exact, and fuzzy name search.

Whole .csv report is too large to adequately search, will limit size of output for the purpose of this early testing

FROM OBSERVATION:
Nearly every single match is a name_fuzzy match between two individuals with the same name but different emails and phone numbers. Here's one example:
Alex,2025-06-27,alexfriedman89@gmail.com,7327968855,Alex,2025-01-06,fblue3504@gmail.com,8592509010,name_fuzzy,"""alex"" ≈ ""alex""",1.0

IDEAS:
- Fuzzy name match is a bad sole decider for duplicate, as many people share names. thinking about where this fits in the big picture might be difficult. Contemplate when/why/how duplicates get into the system, ask around in sales, Stephen, etc. Let's get some examples of duplicates.
- Find out how far back in time duplicates really matter. Most examples I've been given so far of duplicates being problematic is in figuring out meetings statistics for the future. Outdated leads are not likely needed here.

AFTER SOME DIGGING:
- There likely isn't a "lifespan" on leads. Old leads that have already been "won" still get communications from sales, so the problem is more nuanced than that. Here's ideas:
    - Figure out what traits upcoming leads have that end up in the sales team's schedules. Ideally we want this system to automatically handle duplicates, but that may be difficult if there's no consistent traits that identify two leads as duplicates or simply two people that coincidentally have the same name.

NEXT STEPS:
- Get info about the qualities of the bad data we need to avoid:
    - Ask Stephen: How far back of leads really matter?
    - Ask Stephen: Could I get in touch with anybody in sales for further guidance on what they struggle with in regards to duplicates in the CRM?
- Research potential superior methods of locating duplicates.
    - Brainstorm and write down all possible "avenues" for a duplicate to be created. With managing duplicate meetings in the future in mind.
- MAYBE get guidance on how the CRM works and how it gets ahold of leads:
    - How can I find the source of a lead; Typeform, Calendly, Zapier, etc.
    - What qualifies as a duped meeting time? Same day? Multiple meeting times for a single person, period?
    - There might be some special leads that don't follow the rules. "Quentin" seems to be strange, and it has an enormous number of booked meetings.

## TEST 2 2026/04/08
Unmodified Claude-written program. Uses three dupe checks: email exact, phone number exact, and fuzzy name search. Finds lead and meeting dupes. Meeting dupe checks two cases, same_lead and cross_lead:
- same_lead: one lead has 2+ meetings.
- cross_lead: two different leads have 1+ meeting and they share an email or phone number. No overlap with same_lead.

Type of duplicate distribution: 68,743 total found.
LEADS:
Email exact: 4,350 pairs
Phone exact: 4,634 pairs
Name fuzzy: 2,173,623 pairs (threshhold=85)
Total unique duplicate pairs: 2,174,836
- This is only 1,213 pairs higher than name fuzzy pairs.
MEETINGS: 13,583 total found
same_lead: 122,785
cross_lead: 1,923
Total duplicate pairs: 124,708

Interesting data points:
- Quentin single-handedly accounts for 112,574 of the duplicate pairs.
- Based off of some brief observation, the cross_lead dupes seem to be reliable indicators of a duplicate lead and meeting. Which isn't surprising.
- Only 217 dupes found due to a matching phone number. I would guess that this is because emails are checked first and the program only outputs one reason for the duplicate. Most email matches probably coincide with phone matches too.

IDEAS:
- Further evidence that high priority to fuzzy name matching causes problems. Let's ask a question, though. Assuming that a lead's name is input correctly, why do we need fuzzy name matching?

NEXT STEPS:
- Change fuzzy match so that it doesn't hit as often, just try exact matching phone numbers and emails first.
- Limit lead intake for faster testing (full database takes ~15 minutes right now), figure out what size still results in good spread of data.
