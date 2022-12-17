"""
<xml>
    <transaction date="%s" reseller-id="%s">
        <transactionId>%s</transactionId>
        <eventId>%s</eventId>
        <organizerId>%s</organizerId>
        <numberOfPurchasedTickets>%s</numberOfPurchasedTickets>
        <totalAmount>%s</totalAmount>
        <salesChannel>%s</salesChannel>
        <customer>
            <firstName>%s</firstName>
            <lastName>%s</lastName>
        </customer>
        <dateCreated>%s</dateCreated>
    </transaction>
</xml>
"""


def convert_row(row):
    return """<transaction date="%s" reseller-id="%s">
    <transactionId>%s</transactionId>
    <eventId>%s</eventId>
    <organizerId>%s</organizerId>
    <numberOfPurchasedTickets>%s</numberOfPurchasedTickets>
    <totalAmount>%s</totalAmount>
    <salesChannel>%s</salesChannel>
    <customer>
        <firstName>%s</firstName>
        <lastName>%s</lastName>
    </customer>
    <dateCreated>%s</dateCreated>
    <resellerId>%s</resellerId>
    <transactionDate>%s</transactionDate>
</transaction>""" % (
        row.transaction_date, row.reseller_id, row.id, row.event_id, row.organizer_id, row.number_of_purchased_tickets,
        row.total_amount, row.sales_channel, row.customer_first_name, row.customer_last_name, row.transaction_date,
        row.reseller_id, row.transaction_date)


def get_xml(df):
    xml = '\n'.join(df.apply(convert_row, axis=1))
    xml = '<xml>\n' + xml + '\n</xml>'
    return xml

# print('\n'.join([convert_row(row) for row in data[1:]]))
