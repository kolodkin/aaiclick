"""End-to-end tests: mage casts level 2 skills against an opponent."""

from .combat import Player, apply_skill
from .skills import MAGE_SKILLS


def _get_skill(name: str):
    for skill in MAGE_SKILLS[2]:
        if skill.name == name:
            return skill
    raise KeyError(name)


def test_storm_reduces_opponent_dice_bonus():
    mage = Player(name="Mage")
    opponent = Player(name="Opponent", dice_bonus=5)

    apply_skill(_get_skill("Storm"), opponent)

    assert opponent.dice_bonus == 3


def test_storm_does_not_affect_caster():
    mage = Player(name="Mage", dice_bonus=10)
    opponent = Player(name="Opponent", dice_bonus=5)

    apply_skill(_get_skill("Storm"), opponent)

    assert mage.dice_bonus == 10


def test_dragon_breath_neutralizes_one_item():
    mage = Player(name="Mage")
    opponent = Player(name="Opponent", items=["Shield", "Sword"])

    apply_skill(_get_skill("Dragon Breath"), opponent)

    assert len(opponent.items) == 1


def test_dragon_breath_neutralizes_last_item():
    mage = Player(name="Mage")
    opponent = Player(name="Opponent", items=["Shield", "Sword"])

    apply_skill(_get_skill("Dragon Breath"), opponent)

    assert opponent.items == ["Shield"]


def test_dragon_breath_on_opponent_with_no_items():
    mage = Player(name="Mage")
    opponent = Player(name="Opponent", items=[])

    apply_skill(_get_skill("Dragon Breath"), opponent)

    assert opponent.items == []


def test_dragon_breath_does_not_affect_caster():
    mage = Player(name="Mage", items=["Staff"])
    opponent = Player(name="Opponent", items=["Shield"])

    apply_skill(_get_skill("Dragon Breath"), opponent)

    assert mage.items == ["Staff"]


def test_full_combat_round_both_skills():
    mage = Player(name="Mage")
    opponent = Player(name="Opponent", dice_bonus=4, items=["Armor", "Helmet"])

    apply_skill(_get_skill("Storm"), opponent)
    apply_skill(_get_skill("Dragon Breath"), opponent)

    assert opponent.dice_bonus == 2
    assert opponent.items == ["Armor"]
