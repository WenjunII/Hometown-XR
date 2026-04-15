"""
Semantic concept anchors for the second-stage similarity scoring.

These sentences describe the *concepts* we're looking for. The multilingual
sentence-transformer model maps them to a shared embedding space, so an
English anchor sentence will match semantically similar content written
in any of 50+ supported languages.

Anchors should be descriptive, natural-sounding sentences. They are encoded
once at startup and compared against candidate paragraphs.
"""

CONCEPT_ANCHORS = [
    # ── Hometown & Place of Origin ───────────────────────────────────────
    "My hometown is the place where I was born and raised. "
    "It shaped who I am and gave me my earliest memories.",

    "I grew up in a small village surrounded by nature. "
    "The streets and houses of my hometown are etched in my memory.",

    "Returning to the town where I spent my childhood fills me "
    "with a deep sense of connection and nostalgia.",

    # ── Childhood & Growing Up ───────────────────────────────────────────
    "Childhood memories of playing in the fields near our family home "
    "stay with me wherever I go.",

    "Growing up in my parents' house, I learned the values and "
    "traditions that would define my life.",

    "The experiences of my early years and upbringing in my "
    "native community formed my identity.",

    # ── Belonging & Community ────────────────────────────────────────────
    "The feeling of belonging to a community and knowing that "
    "you have a place where you are accepted.",

    "Home is not just a building — it is the sense of belonging, "
    "comfort, and safety that comes from being among your own people.",

    "Finding where you truly belong, the place and community "
    "that feels like home to your soul.",

    # ── Roots & Heritage ─────────────────────────────────────────────────
    "My roots run deep in this land. My ancestors lived here "
    "for generations, and their stories are part of who I am.",

    "Understanding your cultural heritage and ancestral origins "
    "gives you a foundation for your identity.",

    "The traditions passed down from our grandparents connect us "
    "to our roots and give meaning to where we come from.",

    # ── Nostalgia & Homecoming ───────────────────────────────────────────
    "After years of living abroad, I feel a deep longing for "
    "my homeland and the simple life I once knew.",

    "Homesickness is a powerful emotion — the ache of missing "
    "the familiar places, sounds, and smells of home.",

    "Coming back to the place where I grew up after many years "
    "brought tears to my eyes and warmth to my heart.",

    # ── Diaspora & Displacement ──────────────────────────────────────────
    "As an immigrant, I carry my homeland within me. "
    "My cultural identity bridges two worlds.",

    "The diaspora experience means being caught between two cultures, "
    "longing for a home that may no longer exist as you remember it.",

    "Being uprooted from your native land and having to rebuild "
    "a sense of home in a foreign country.",

    # ── Concept of Home ──────────────────────────────────────────────────
    "Home is more than a physical place. It is where the heart is, "
    "where you feel safe, loved, and truly yourself.",

    "The meaning of home changes as we grow older, but the longing "
    "for a place to call our own never fades.",
]
