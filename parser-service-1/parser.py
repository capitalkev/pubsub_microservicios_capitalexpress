from lxml import etree
from datetime import datetime, timedelta

def extract_invoice_data(xml_content_bytes: bytes) -> dict:
    """
    Toma el contenido de un archivo XML en bytes, lo parsea y devuelve
    un diccionario con los datos extraídos de la factura.
    Incluye validación robusta para prevenir errores por XMLs malformados.
    """
    REQUIRED_FIELDS = [
        ('.//cbc:ID', 'document_id'),
        ('.//cac:LegalMonetaryTotal/cbc:PayableAmount', 'total_amount'),  
        ('.//cac:AccountingSupplierParty//cac:PartyLegalEntity/cbc:RegistrationName', 'client_name'),
        ('.//cac:AccountingCustomerParty//cac:PartyLegalEntity/cbc:RegistrationName', 'debtor_name'),
        ('.//cbc:IssueDate', 'issue_date')
    ]
    
    VALID_CURRENCIES = {'PEN', 'USD', 'EUR'}
    
    try:
        # Decodificación robusta con múltiples encodings
        xml_content = None
        root = None
        
        for encoding in ['iso-8859-1', 'utf-8', 'cp1252']:
            try:
                xml_content = xml_content_bytes.decode(encoding).lstrip('\ufeff')
                root = etree.fromstring(xml_content.encode('utf-8'))
                break
            except (UnicodeDecodeError, etree.XMLSyntaxError):
                continue
        
        if root is None:
            return {"error": "XML con encoding no válido o malformado", "valid": False}
            
    except Exception as e:
        return {"error": f"Error al decodificar XML: {str(e)}", "valid": False}

    ns = {
        'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
        'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2'
    }

    # Validar namespace correcto
    root_namespace = root.nsmap.get(None)
    if root_namespace != 'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2':
        return {"error": f"XML no es factura UBL válida. Namespace: {root_namespace}", "valid": False}

    # Validar campos obligatorios
    for xpath, field_name in REQUIRED_FIELDS:
        element = root.find(xpath, ns)
        if element is None or not (element.text and element.text.strip()):
            return {"error": f"Campo obligatorio faltante o vacío: {field_name} ({xpath})", "valid": False}

    # Validar moneda
    currency_element = root.find('.//cac:LegalMonetaryTotal/cbc:PayableAmount', ns)
    currency = currency_element.get('currencyID', 'N/A') if currency_element is not None else 'N/A'
    
    if currency not in VALID_CURRENCIES:
        return {"error": f"Moneda no válida: {currency}. Válidas: {VALID_CURRENCIES}", "valid": False}

    def find_text(xpath, default=None):
        element = root.find(xpath, ns)
        return element.text.strip() if element is not None and element.text is not None else default

    # Extracción de datos
    issue_date_str = find_text('.//cbc:IssueDate')
    total_amount = float(find_text('.//cac:LegalMonetaryTotal/cbc:PayableAmount', '0'))
    payment_form = find_text(".//cac:PaymentTerms[cbc:ID='FormaPago']/cbc:PaymentMeansID")
    due_date_str = find_text('.//cac:PaymentTerms/cbc:PaymentDueDate')
    
    # Lógica de fechas
    issue_date = datetime.strptime(issue_date_str, '%Y-%m-%d') if issue_date_str else None
    due_date = None
    if due_date_str:
        due_date = datetime.strptime(due_date_str, '%Y-%m-%d')
    elif payment_form and payment_form.lower() == 'contado' and issue_date:
        due_date = issue_date + timedelta(days=60)
    else:
        due_date = issue_date

    issue_date_iso = issue_date.isoformat() if issue_date else None
    due_date_iso = due_date.isoformat() if due_date else None

    currency_element = root.find('.//cac:LegalMonetaryTotal/cbc:PayableAmount', ns)
    currency = currency_element.get('currencyID', 'N/A') if currency_element is not None else 'N/A'
    detraction_amount = float(find_text(".//cac:PaymentTerms[cbc:ID='Detraccion']/cbc:PaymentPercent", '0'))
    net_amount = total_amount * (100 - detraction_amount) / 100

    invoice_data = {
        "document_id": find_text('./cbc:ID'),
        "issue_date": issue_date_iso,
        "due_date": due_date_iso,
        "currency": currency,
        "total_amount": total_amount,
        "net_amount": net_amount,
        "debtor_name": find_text('.//cac:AccountingCustomerParty//cac:PartyLegalEntity/cbc:RegistrationName'),
        "debtor_ruc": find_text('.//cac:AccountingCustomerParty//cac:PartyIdentification/cbc:ID'),
        "client_name": find_text('.//cac:AccountingSupplierParty//cac:PartyLegalEntity/cbc:RegistrationName'),
        "client_ruc": find_text('.//cac:AccountingSupplierParty//cac:PartyIdentification/cbc:ID'),
        "valid": True  # Marcar como válido si llegó hasta aquí
    }
    
    return invoice_data