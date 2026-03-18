"""End-to-end tests: mage casts level 2 skills against an opponent."""

from .combat import Player, apply_skill
from .skills import MAGE_SKILLS


def test_full_combat_round_both_skills():
    mage = Player(name="Mage")
    opponent = Player(name="Opponent", dice_bonus=4, items=["Armor", "Helmet"])

    for skill in MAGE_SKILLS[2]:
        apply_skill(skill, opponent)

    assert opponent.dice_bonus == 2
    assert opponent.items == ["Armor"]
