from orders.models import Order, OrderDetail
from orders.service import OrdersService

def test_can_create_order(db_session):
    order = Order()
    db_session.add(order)
    db_session.commit()
    assert order.id > 0


def test_can_create_order_detail(db_session):
    order = Order()
    order_detail_1 = OrderDetail(
        order=order,
        product_id="the_enigma",
        price=100.50,
        quantity=1
    )
    order_detail_2 = OrderDetail(
        order=order,
        product_id="the_odyssey",
        price=99.50,
        quantity=2
    )

    db_session.add_all([order_detail_1, order_detail_2])
    db_session.commit()

    assert order.id > 0
    for order_detail in order.order_details:
        assert order_detail.id > 0
    assert order_detail_1.product_id == "the_enigma"
    assert order_detail_1.price == 100.50
    assert order_detail_1.quantity == 1
    assert order_detail_2.product_id == "the_odyssey"
    assert order_detail_2.price == 99.50
    assert order_detail_2.quantity == 2

def test_list_orders(db_session):
    # Create sample orders in the database
    orders = [
        Order(),
        Order(),
        Order(),
    ]
    db_session.add_all(orders)
    db_session.commit()

    # Retrieve all orders from the database
    result = db_session.query(Order).all()

    # Assert that the number of retrieved orders is equal to the number of inserted orders
    assert len(result) == len(orders)