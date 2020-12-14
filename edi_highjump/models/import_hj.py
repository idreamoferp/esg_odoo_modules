from odoo import api, fields, models
import pyodbc 
import logging, time
from datetime import datetime
_logger = logging.getLogger("HighJump")

class edi_highjump_import(models.Model):
    _name = "edi.highjump"
    _description = "EDI HighJump"
    
    def open_cursor(self):
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=ECCOSQL01\HJ;Database=AAD;UID=hjepoxy;PWD=Xppxu5Q0VBvRnT6h')
        cursor = conn.cursor()
        return cursor
        
    def import_highjump(self):
        connection = self.open_cursor()
        
        self.import_item_master(connection)
        self.import_bom_master(connection)
        self.import_bom_detail(connection)
        self.import_users(connection)
        self.import_locations(connection)
        self.import_vendors(connection)
        
        # self.inventory_adjustment(connection)
        return True
    
    def import_highjump_txn(self):
        # hj_import = self.env.ref('edi_highjump.highjump_cron')
        
        #2020-8-25 15:51:58
        connection = self.open_cursor()
        config_last_date = self.env.ref('edi_highjump.highjump_last_date')
        last_date = datetime.strptime(config_last_date.value, '%Y-%m-%d %H:%M:%S')
        dates = []
        # dates.append(self.import_po_master(connection, last_date))
        dates.append(self.import_mo_master(connection, last_date))
        dates.append(self.import_tran_log(connection, last_date))
        
        
        config_last_date.value = min(dates)
        connection.close()
        return True
    
    def import_item_master(self, connection):
        sql = "SELECT item_number,description,uom,inventory_type,alt_item_number,std_qty_uom,qty_on_hand,unit_weight,country_of_origin,work_center,part_status,part_type,buyer_id,std_unit_cost,commodity_description"
        #sql +=",carton_label_file,master_carton_label,m_label,ctn_upc_code,ctn_desc_1,ctn_desc_2,ctn_desc_3,ctn_desc_4,mc_upc_code,mc_qty,mc_desc_1,mc_desc_2,mc_desc_3,mc_desc_4,date_code_label"
        # sql += " FROM (SELECT *, ROW_NUMBER() OVER ( ORDER BY item_number ) AS RowNum FROM t_item_master) AS RowConstrainedResult"
        # sql += " WHERE RowNum BETWEEN 58000 AND 58500 AND item_number ='PIC16C57-XT/SP'"
        sql += " FROM t_item_master"
        sql +=";"
        
        items = connection.execute(sql).fetchall()
        _product = self.env['product.template']
        
        for item in items:
            try:
                xmlid = '%s_item_master_%s' % ("aad", item[0])
                
                #find item_number in external reference database
                product = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
                
                if product is None:
                    #time to create a new product =D
                    name = item[1]
                    if name == None:
                        name = item[0]
                        
                    product = _product.create({'default_code':item[0], 'name':name,})
                    x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'product.template', 'res_id':product.id, 'name':xmlid, 'noupdate':True})
                    _logger.info("Create new Product from Item Master [%s]" % (item[0]))
                
                
                is_updated = False
                
                name = item[1]
                if name == None:
                    name = item[0]
                        
                if product.name != name:
                    product.name = name
                    is_updated =True
                
                p_type = ""    
                if item[3] == "FG":
                    p_type = 'product'
                    
                if item[3] == "MS":
                    p_type = 'product'
                    
                if product.type != p_type:
                    product.type = p_type
                    is_updated =True
                
                weight = item[7]
                if weight == None:
                    weight=0.0
                    
                weight = round(weight / 2.205, 2)
                if product.weight != weight:
                    product.weight=weight
                    is_updated =True
                    
                #status
                p_status = True
                if item[10] == "O":
                    p_status = False
                
                if product.active != p_status:
                     product.active = p_status
                     is_updated =True
                
                if item[11] == "M":
                    if product.purchase_ok != False:
                        #remove "can be purchased flag, this removes the item from purchase order searches"
                        product.purchase_ok = False
                        is_updated = True
                        
                    stock_route_buy = self.env.ref("purchase_stock.route_warehouse0_buy")
                    stock_route_make = self.env.ref("mrp.route_warehouse0_manufacture")
                    
                    if stock_route_make not in product.route_ids:
                        #adds make to the routeing list for this product
                        product.route_ids = [(4, stock_route_make.id)]
                        is_updated=True
                    if stock_route_buy in product.route_ids:
                        #removes buy fromt he routing list
                        product.route_ids = [(3, stock_route_buy.id)]
                        is_updated=True
                        
                if item[11] == "B":
                    if product.purchase_ok != True:
                        #Sets can be purchased flag, this removes the item from purchase order searches
                        product.purchase_ok = True
                        is_updated = True
                        
                    stock_route_buy = self.env.ref("purchase_stock.route_warehouse0_buy")
                    stock_route_make = self.env.ref("mrp.route_warehouse0_manufacture")
                    
                    if stock_route_make in product.route_ids:
                        #removes make to the routeing list for this product
                        product.route_ids = [(3, stock_route_make.id)]
                        is_updated=True
                    if stock_route_buy not in product.route_ids:
                        #adds buy fromt he routing list
                        product.route_ids = [(4, stock_route_buy.id)]
                        is_updated=True
                        
                product_uom_id = self.env.ref('edi_highjump.%s_uom_%s' % ("aad", item[2]) ,raise_if_not_found=False)
                if product_uom_id is not None:
                    if product.uom_id != product_uom_id:
                        product.uom_id = product_uom_id
                        product.uom_po_id = product_uom_id
                        
                        is_updated =True
                
                if is_updated:
                    _logger.info("Updated Product from Item Master [%s]" % (item[0]))
                     
            except Exception as e:
                _logger.error("Error %s while importing [%s]" % (e, item[0]))
                pass
                
            
        pass
    
    def import_bom_master(self, connection):
        sql = ""
        sql += "SELECT t_bom_master.kit_id, t_bom_master.description ,t_bom_master.status, t_bom_master.created_date, t_bom_master.lwl_file, t_item_master.work_center"
        sql += " FROM t_bom_master"
        sql += " INNER JOIN t_item_master ON t_bom_master.kit_id=t_item_master.item_number"
        #sql += " WHERE kit_id like '7945%'"
        
        items = connection.execute(sql).fetchall()
        _bom = self.env['mrp.bom']
        for item in items:
            is_updated = False
            xmlid = '%s_bom_master_%s' % ("aad", item[0])
            
            #find item_number in external reference database
            bom = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
            
            if bom is None:
                product_tmpl_id = self.env.ref('edi_highjump.%s_item_master_%s' % ("aad", item[0]))
                bom = _bom.create({'product_tmpl_id':product_tmpl_id.id, 'product_qty':1, 'ready_to_produce':'all_available', 'type':'normal'})
                x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'mrp.bom', 'res_id':bom.id, 'name':xmlid, 'noupdate':True})
                _logger.info("Create new BOM from bom_master [%s]" % (item[0]))
                
            #status
            p_status = True
            if item[2] != "A":
                p_status = False
            
            if bom.active != p_status:
                 bom.active = p_status
                 is_updated =True
            
            if item[5] is not None:
                bird =  bom.routing_id
                #sync route for this product
                if len(bom.routing_id) == 0:
                    #create a routing for this BOM
                    workcenter_id = self.mrp_workcell(item[5])
                    this=1
                    operation = {'name':item[5], 'workcenter_id':workcenter_id.id,}
                    route_id = self.env['mrp.routing'].create({'name':item[0], 'operation_ids':[(0,0,operation),], })
                    bom.routing_id = route_id
                    is_updated =True
                    _logger.info("Created Routing for BOM %s" % item[0])
                
            if bom.product_uom_id.category_id != bom.product_tmpl_id.uom_id.category_id:
                bom.product_uom_id = bom.product_tmpl_id.uom_id
                is_updated =True
                
            if is_updated:
                _logger.info("Updated BOM from bom_master [%s]" % (item[0]))
        pass
    
    def import_bom_detail(self, connection):
        sql = ""
        sql += "SELECT kit_id, item_number, quantity"
        sql += " FROM t_bom_detail "
       
        
        for item in connection.execute(sql).fetchall():
            is_updated = False
            if item[2] < 1:
                #will need to fix this later :()
                continue
            
            xmlid = '%s_bom_detail_%s_%s' % ("aad", item[0], item[1])
            
            bom_line = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
            product_tmpl_id = self.env.ref('edi_highjump.%s_item_master_%s' % ("aad", item[1]),raise_if_not_found=False)
            if product_tmpl_id is None:
                _logger.error("Could not find raw material %s" % (item[1]),)
                continue            
            
            if bom_line is None:
                
                
                variant_id = self.env['product.product'].search(['|',('active','=',True),('active','=',False),('product_tmpl_id','=',product_tmpl_id.id),])
                bom_id = self.env.ref('edi_highjump.%s_bom_master_%s' % ("aad", item[0]))
                
                new_line = {'bom_id':bom_id.id, 'product_id':variant_id.id, 'product_qty':item[2], 'product_uom_id':product_tmpl_id.uom_id.id}
                bom_line = self.env['mrp.bom.line'].create(new_line)
                x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'mrp.bom.line', 'res_id':bom_line.id, 'name':xmlid, 'noupdate':True})
                _logger.info("Create new BOM LINE from bom_detail [%s]" % (item[0]))
                
            if float(bom_line.product_qty) != float(item[2]):
                bom_line.product_qty = item[2]
                is_updated = True
                
            if bom_line.product_uom_id != product_tmpl_id.uom_id:
                bom_line.product_uom_id = product_tmpl_id.uom_id
                is_updated = True
                
            
            if is_updated:
                _logger.info("Updated BOM LINE from bom_detail [%s]" % (item[0]))
                
    def mrp_workcell(self, workcell):
        xmlid = '%s_workcell_%s' % ("aad", workcell)
            
        #find item_number in external reference database
        workcenter_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
        
        if workcenter_id is None:
                workcenter_id = self.env['mrp.workcenter'].create({'name':workcell, 'code':workcell, 'active':True})
                x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'mrp.workcenter', 'res_id':workcenter_id.id, 'name':xmlid, 'noupdate':True})
                _logger.info("Create new WORKCENTER from workcell [%s]" % (workcell))
        
        return workcenter_id
        
    def import_users(self, connection):
        sql = ""
        sql += "SELECT id,name,password,dept,status"
        sql += " FROM t_employee"
       
        
        for item in connection.execute(sql):
            is_updated = False
            
            try:
                xmlid = '%s_employee_%s' % ("aad", item[0])
                
                #find item_number in external reference database
                user_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
                
                if user_id is None:
                    u = {'login':item[0],'partner_id':self.env['res.partner'].create({'name':item[1], 'type':'contact',}).id, }
                    user_id = self.env['res.users'].create(u)
                    x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'res.users', 'res_id':user_id.id, 'name':xmlid, 'noupdate':True})
                    _logger.info("Create new USER from employee [%s]" % (item[0]))
                        
                active = False
                if item[4] == 'A':
                    active = True
                    
                if user_id.active != active:
                    user_id.active = active
                    is_updated = True
                    
                # odoo 13.0+ no longer uses customer and vendor flags    
                # if user_id.customer == True:
                #     user_id.customer =False
                #     is_updated = True
                    
                # if user_id.supplier == True:
                #     user_id.supplier =False
                #     is_updated = True
                
                name = item[1].split(",")
                if len(name) == 2:
                    item[1] = name[1] + " " + name[0]
                    
                if user_id.name != item[1]:
                    user_id.name = item[1]
                    user_id.partner_id.name = item[1]
                    is_updated = True
                    
                if is_updated:
                    _logger.info("Updated USER from employee [%s]" % (item[0]))
                    
            except Exception as e:
                _logger.error("Error %s while importing [%s]" % (e, item[0]))
                pass
                    
        pass
    
    def import_locations(self, connection):
        sql = ""
        sql += "SELECT location_id,description,type,status"
        sql += " FROM t_location"
       
        loc_stock =  self.env.ref('stock.stock_location_stock')
        loc_fork = self.env.ref('edi_highjump.highjump_stock_fork')
        loc_prodution = self.env.ref('edi_highjump.highjump_stock_prod')
        
        for item in connection.execute(sql).fetchall():
            is_updated = False
            
            try:
                xmlid = '%s_location_%s' % ("aad", item[0])
                
                #find item_number in external reference database
                location_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
                
                if location_id is None:
                    u = {'name':item[0],'useage':'internal', 'location_id':loc_stock.id }
                    location_id = self.env['stock.location'].create(u)
                    x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'stock.location', 'res_id':location_id.id, 'name':xmlid, 'noupdate':True})
                    _logger.info("Create new LOCATION from location [%s]" % (item[0]))
                        
                stat = True
                if item[3] == "I":
                    stat = False
                   
                    
                if location_id.active != stat:
                    location_id.active = stat
                    is_updated = True
                
                if item[2] == "F":
                    #for type location
                    if location_id.location_id != loc_fork:
                        location_id.location_id = loc_fork
                        is_updated = True
                    
                if item[2] == "W":
                    #for type location
                    if location_id.location_id != loc_prodution:
                        location_id.location_id = loc_prodution
                        is_updated = True
                    
                
                if is_updated:
                    _logger.info("Updated STOCK LOCATION from locations [%s]" % (item[0]))
                    
            except Exception as e:
                _logger.error("Error %s while importing [%s]" % (e, item[0]))
                pass
                    
        pass
    
    def import_vendors(self, connection):
        sql = ""
        sql += "SELECT vendor_id,vendor_code,vendor_name,status,grower_vendor,po_vendor,inspection_code"
        sql += " FROM t_vendor"
       
        
        for item in connection.execute(sql):
            is_updated = False
            
            try:
                xmlid = '%s_vendor_%s' % ("aad", item[0].strip())
                
                #find item_number in external reference database
                vendor_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
                
                if vendor_id is None:
                    u = {'name':item[2],'supplier':True , 'customer':False, 'ref':item[1].strip(), 'company_type':'company' }
                    vendor_id = self.env['res.partner'].create(u)
                    x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'res.partner', 'res_id':vendor_id.id, 'name':xmlid, 'noupdate':True})
                    _logger.info("Create new PARTNER from vendor [%s]" % (item[0].strip()))
                        
                stat = True
                if item[3] == "I":
                    stat = False
                    
                     
                if vendor_id.active != stat:
                    vendor_id.active = stat
                    is_updated = True
                
                # x = self.env['ir.model.data'].xmlid_lookup('edi_highjump.%s' % xmlid)
                    
                # y = self.env['ir.model.data'].browse(x[0])[0]
                # y.name = 'aad_vendor_%s' % item[0].strip()
                    
                if is_updated:
                    _logger.info("Updated PARTNER from vendor [%s]" % (item[0].strip()))
                    
            except Exception as e:
                _logger.error("Error %s while importing [%s]" % (e, item[0]))
                pass
                    
        pass
    
    def import_po_master(self, connection, last_date):
        sql = ""
        sql += "SELECT po_number,vendor_code,create_date,status"
        sql += " FROM t_po_master"
        sql += " WHERE create_date > '%s'" % last_date
        sql += " order by create_date"
       
        items = connection.execute(sql).fetchall()
        
        for item in items:
            is_updated = False
            
            try:
                xmlid = '%s_po_master_%s' % ("aad", item[0])
                
                #find item_number in external reference database
                po_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
                
                if po_id is None:
                    partner_id =  self.env.ref('edi_highjump.aad_vendor_%s' % (item[1])) #self.env['res.partner'].search([('ref','=',item[1])])
                    u = {'name':item[0], 'date_order':item[2] , 'partner_id':partner_id.id}
                    po_id = self.env['purchase.order'].create(u)
                    x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'purchase.order', 'res_id':po_id.id, 'name':xmlid, 'noupdate':True})
                    _logger.info("Create new PO from po_master [%s]" % (item[0]))
                    
                if po_id.name != item[0]:
                    po_id.name = item[0]
                    is_updated = True
                    
                sql = ""
                sql += "SELECT line_number,item_number,qty,vendor_item_number,delivery_date,originator,wh_id,order_uom,line_status,stock_flag,rcpt_overage"
                sql += " FROM t_po_detail"
                sql += " WHERE po_number = '%s'" % (po_id.name)
                po_lines = connection.execute(sql)
                for po_line in po_lines:
                    xmlid = '%s_po_detail_%s_%s' % ("aad", po_id.name, po_line[0])
                    po_line_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
                    
                    if po_line_id is None:
                        product_tmpl_id = self.env.ref('edi_highjump.%s_item_master_%s' % ("aad", po_line[1]),raise_if_not_found=False)
                        variant_id = self.env['product.product'].search(['|',('active','=',True),('active','=',False),('product_tmpl_id','=',product_tmpl_id.id),])
                        
                        uom = self.env.ref('edi_highjump.%s_uom_%s' % ("aad", po_line[7]) ,raise_if_not_found=False)
                        if uom is None:
                            uom = product_tmpl_id.uom_id
                            
                        if product_tmpl_id.uom_id.category_id != uom.category_id:
                            uom = product_tmpl_id.uom_id
                        
                        line = {"order_id":po_id.id, "product_id":variant_id.id, 'product_qty':po_line[2], 'name':po_line[2], 'date_planned':po_line[4], 'product_uom':uom.id, 'price_unit':0.00}
                        po_line_id = self.env['purchase.order.line'].create(line)
                        x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'purchase.order.line', 'res_id':po_line_id.id, 'name':xmlid, 'noupdate':True})
                        _logger.info("Create new PO LINE from po_detail [%s - %s]" % (po_id.name, po_line[1]))
                    
                    if po_line[3] is None:
                        po_line[3] = po_line[1]
                        
                    if po_line_id.name != po_line[3]:
                        po_line_id.name = po_line[3]
                        is_updated = True
                        
                    if po_line_id.sequence != int(po_line[0]):
                        po_line_id.sequence = int(po_line[0])
                        is_updated = True
                
                if po_id.state == 'draft':
                    #check for inventory tnxs for this po
                    sql = ""
                    sql += "SELECT TOP (1) log_id"
                    sql += " FROM t_tran_log"
                    sql += " WHERE return_disposition = '%s'" % item[0]
        
                    trans_log = connection.execute(sql).fetchall()
                    
                    if len(trans_log) > 0:
                        #there are transfers in the trans_log, lets go ahead and confirm this PO
                        po_id.button_confirm()
                        _logger.info("Confirm PO [%s]" % (item[0]))
                    
                if is_updated:
                    _logger.info("Updated PO from po_master [%s]" % (item[0]))
                            
            except Exception as e:
                _logger.error("Error %s while importing [%s]" % (e, item[0]))
                
            last_date = item[2] 
        return last_date
    
    def import_mo_master(self, connection, last_date):
        sql = ""
        sql += "SELECT TOP (1000) work_order_number, item_number, status, qty_ordered, date_created, source, operator, comment, work_center"
        sql += " FROM t_work_order_master"
        sql += " WHERE date_created > '%s' AND qty_ordered > 0 AND item_number != 'None'" % last_date
        sql += " order by date_created"
        
        for item in connection.execute(sql).fetchall():
            is_updated = False
            try:
                xmlid = '%s_wo_master_%s' % ("aad", item[0])
                
                #find item_number in external reference database
                mo_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
                
                if mo_id is None:
                    product_tmpl_id = self.env.ref('edi_highjump.%s_item_master_%s' % ("aad", item[1]),raise_if_not_found=False)
                    if product_tmpl_id is None:
                        _logger.error("Product %s not found while importing [%s]" % (item[1], item[0]))
                        continue
                    if len(product_tmpl_id.bom_ids) == 0:
                        _logger.error("BOM for product %s not found while importing [%s]" % (item[1], item[0]))
                        continue
                    
                    variant_id = self.env['product.product'].search(['|',('active','=',True),('active','=',False),('product_tmpl_id','=',product_tmpl_id.id),])
                    bom_id = self.env['mrp.bom']._bom_find(product=variant_id)
                    dest_location = self.env.ref('edi_highjump.%s_location_%s' % ("aad", item[8]))
                    
                    u = {'name':item[0], 'product_id':variant_id.id, 'bom_id':bom_id.id, 'date_planned_start':item[4] , 'origin':item[5], 'product_qty':item[3], 'product_uom_id':variant_id.uom_id.id, 'location_src_id':dest_location.id, 'location_dest_id':dest_location.id}
                    
                    mo_id = self.env['mrp.production'].create(u)
                    
                    x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'mrp.production', 'res_id':mo_id.id, 'name':xmlid, 'noupdate':True})
                    _logger.info("Create new MO from wo_master [%s]" % (item[0]))
                
                user_id =  self.env.ref('edi_highjump.aad_employee_%s' % (item[6]), raise_if_not_found=False)
                if user_id is not None:
                    if mo_id.user_id != user_id:
                        mo_id.user_id = user_id
                        is_updated = True
                
                # if item[1] in ['520-FORD3','530-FORD5']:
                #     mo_id.button_plan()
                    
                if is_updated:
                    _logger.info("Updated MO from wo_master [%s]" % (item[0]))
                
                #mo_id.do_unreserve()
            except Exception as e:
                _logger.error("Error %s while importing [%s]" % (e, item[0]))
                break
            last_date = item[4]
            
        return last_date
            
    def inventory_adjustment(self, connection):
        sql = ""
        sql += "SELECT item_number, actual_qty, status, location_id, fifo_date"
        sql += " FROM t_stored_item"
        sql += " where actual_qty >= 0"
        
        loc_stock =  self.env.ref('stock.stock_location_stock')
        
        #adjustment_id = self.env['stock.inventory'].create({'name':'HJ Adjustment', 'date':datetime.now(), 'filter':'none', 'location_id':loc_stock.id, 'state':'draft',})
        #adjustment_id.action_start()
        adjustment_id = self.env['stock.inventory'].browse(6)
        adjustment_id.action_reset_product_qty()
        
        new_lines = []
        for item in connection.execute(sql).fetchall():
            product_tmpl_id = self.env.ref('edi_highjump.%s_item_master_%s' % ("aad", item[0]))
            variant_id = self.env['product.product'].search(['|',('active','=',True),('active','=',False),('product_tmpl_id','=',product_tmpl_id.id),])
            location_id = self.env.ref('edi_highjump.%s_location_%s' % ("aad", item[3]))
            
            inventory_line = adjustment_id.line_ids.filtered(lambda q: q.product_id == variant_id and q.location_id == location_id)
            if not inventory_line:
                try:
                    adjustment_id.line_ids = [  (0,0,{'product_id':variant_id.id, 'product_uom_id':variant_id.uom_id.id, 'product_qty':float(item[1]), 'location_id':location_id.id,}),  ]
                except Exception as e:
                    pass
                continue
            
            inventory_line.product_qty += float(item[1])
            
            if inventory_line.product_qty == inventory_line.theoretical_qty:
                inventory_line.unlink()
                
       
        #adjustment_id.action_validate()
        return True
        
    def get_lpn(self, lpn):
        package = self.env['stock.quant.package'].search([('name','=',lpn)])
        if len(package) == 0:
            package = self.env['stock.quant.package'].create({'name':lpn})
            
        return package
    
   
    def import_tran_log(self, connection, last_date):
        my_last_date = last_date
        
        
        sql = ""
        sql += "SELECT top (25000) log_id, tran_type, start_tran_date, start_tran_time, employee_id, control_number, line_number, control_number_2, outside_id, location_id, hu_id, num_items,"
        sql += " item_number, tran_qty, location_id_2, hu_id_2, return_disposition"
        sql += " FROM t_tran_log"
        sql += " WHERE start_tran_date >= '%s' AND tran_type IN (109, 110, 112, 114, 136, 138, 139, 201, 202, 231, 232, 251, 252, 253, 254, 301, 302, 303, 304, 391, 392, 395, 396)" % (last_date.date())
        sql += " ORDER BY start_tran_date asc, start_tran_time asc, tran_type desc;"
        
        tran_lines =  connection.execute(sql).fetchall()
        line_num = 0
        for tran_line in tran_lines:
            tran_error = False
            tran_date = tran_line[2].strftime("%Y/%m/%d") + tran_line[3].strftime(" %H:%M:%S")
            tran_date = datetime.strptime(tran_date, '%Y/%m/%d %H:%M:%S')
            if tran_date <= last_date:
                continue
            
            
            if tran_line[1] in ('201', '202', '231', '232', '251', '252', '253', '254', '301', '302', '303', '304', '391', '392', '395', '396'):
                tran_error = self.import_tran_internal(tran_line)
                
            
            if tran_line[1] == '114':
                tran_error =self.import_tran_114(tran_line)
                
            if tran_line[1] in ('109', '136', '139'):
                
                #tester bachflush uses outsideID ref as return disposition 
                if tran_line[1] == '139':
                    tran_line[16] = self.get_trav_num(connection, tran_line[8])
                    
                tran_error =self.import_tran_mfg_consume(tran_line)
        
            if tran_line[1] in ('110', '112', '138'):
                
                #tester bachflush uses outsideID as return disposition 
                if tran_line[1] == '138':
                    tran_line[16] = self.get_trav_num(connection, tran_line[8])
                tran_error =self.import_tran_mfg_produce(tran_line) 
            
            if not tran_error:
                break
                
            my_last_date = tran_date
            drinkme=1
            
            line_num += 1
            if line_num >= 500:
                break
        
        return my_last_date
        
    def import_tran_PO_tran(self, tran_line):
        is_updated = False
        
        # try:
            
        po_line_id = self.env.ref('edi_highjump.%s_po_detail_%s_%s' % ('aad', tran_line[16], tran_line[6]))
        xmlid = '%s_tran_log_%s' % ("aad", tran_line[0])
        #find item_number in external reference database
        move_line_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
        
        if move_line_id is None:
            #check it the po is approved, if not approve it
            if po_line_id.order_id.state == 'draft':
                po_line_id.order_id.button_confirm()
                
            #update the stock move for this po_line to receive the qty listed on the txn
            dest_location = self.env.ref('edi_highjump.%s_location_%s' % ('aad', tran_line[14]))
            
            
            if not po_line_id.move_ids.filtered(lambda move: move.state != 'done').move_line_ids:
                new_move = po_line_id._prepare_stock_moves()
                drink_me=1
            
            
            move_line_id = po_line_id.move_ids.filtered(lambda move: move.state != 'done').move_line_ids
            
            
            if move_line_id[0].qty_done == 0:  
                move_line_id.qty_done = tran_line[13]
                move_line_id.date = tran_line[2]
                move_line_id.location_dest_id = dest_location
                
            if move_line_id[0].qty_done > 0:
                vendor_location = self.env.ref('stock.stock_location_suppliers',)
                #this line has been entered, create a new line
                new_line = move_line_id[0].copy_data()[0]
                new_line['qty_done'] = tran_line[13]
                new_line['date'] = tran_line[2]
                new_line['location_dest_id'] = dest_location.id
                #add new move line to the move
                move_line_id = self.env['stock.move.line'].create(new_line)
                
                if tran_line[15] is not None:
                    move_line_id.result_package_id = self.get_lpn(tran_line[15])
            
            move_line_id.move_id._action_done()
            
            #save a reference to this move line in the database       
            x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'stock.move.line', 'res_id':move_line_id.id, 'name':xmlid, 'noupdate':True})         
            _logger.info("PO %s Recive %s DONE!" % (tran_line[16], tran_line[12])  )
        
        # if move_line_id.state != 'done':
        #   move_line_id.picking_id.action_done()
            
        # #try to close the pick list for this PO if all the inventory has been received perfectly
        # if len(po_line_id.order_id.picking_ids) > 0:
        #     try:
        #         picking_lines = po_line_id.order_id.picking_ids[0].move_lines.filtered(lambda move: move.product_qty != move.quantity_done)
        #         if len(picking_lines) == 0:
        #             po_line_id.order_id.picking_ids[0].button_validate()
        #             _logger.info("Validate and Close [%s]" % (po_line_id.order_id.picking_ids[0].name))
                    
        #     except Exception as e:
        #         _logger.error("Error %s while Closing [%s]" % (e, tran_line[0]))
        
        
        if is_updated:
            _logger.info("Updated STOCK MOVE from tran_log [%s]" % (tran_line[0]))
            
        # except Exception as e:
        #     _logger.error("Error %s while importing [%s]" % (e, tran_line[0]))  
        
        return True   
    
    def import_tran_114(self, tran_line):
        update = False
        # try:
        tran_date = tran_line[2].strftime("%Y/%m/%d") + tran_line[3].strftime(" %H:%M:%S")
        tran_date = datetime.strptime(tran_date, '%Y/%m/%d %H:%M:%S')
        
        xmlid = '%s_tran_log_%s' % ("aad", tran_line[0])
        #find item_number in external reference database
        move_line_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
        
        if move_line_id is None:
            
            product_tmpl_id = self.env.ref('edi_highjump.%s_item_master_%s' % ("aad", tran_line[12]))
            variant_id = self.env['product.product'].search(['|',('active','=',True),('active','=',False),('product_tmpl_id','=',product_tmpl_id.id),])
            
            
            name = "%s_%s" % ( tran_line[16], tran_line[6])
            vendor_location = self.env.ref('stock.stock_location_suppliers',)
            location_id = self.env.ref('edi_highjump.%s_location_%s' % ('aad', tran_line[14]))
            
            stock_move = self.inventory_move(name, variant_id, vendor_location, location_id, tran_line[13], variant_id.uom_id, tran_date)
            
            move_line_id = stock_move.move_line_ids[0]
            x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'stock.move.line', 'res_id':move_line_id.id, 'name':xmlid, 'noupdate':True}) 
            _logger.info("PO %s Receive %s DONE!" % (tran_line[16], tran_line[12]))
            
            if move_line_id.date != tran_date:
                move_line_id.date = tran_date
                move_line_id.move_id.date = tran_date
                update = True
        return True
        
    def import_tran_mfg_consume(self, tran_line):
        is_updated = False
        
        # try:
        tran_date = tran_line[2].strftime("%Y/%m/%d") + tran_line[3].strftime(" %H:%M:%S")
        tran_date = datetime.strptime(tran_date, '%Y/%m/%d %H:%M:%S')
        
        xmlid = '%s_tran_log_%s' % ("aad", tran_line[0])
        #find item_number in external reference database
        move_line_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
        
        if move_line_id is None:
            mrp_production = self.env.ref('edi_highjump.%s_wo_master_%s' % ('aad', tran_line[16]), raise_if_not_found=False)
            product_tmpl_id = self.env.ref('edi_highjump.%s_item_master_%s' % ("aad", tran_line[12]))
            variant_id = self.env['product.product'].search(['|',('active','=',True),('active','=',False),('product_tmpl_id','=',product_tmpl_id.id),])
            
            if not mrp_production:
                #this traveler didnt make it in the import, treat it as an interal move
                location_id = self.env.ref('edi_highjump.%s_location_%s' % ('aad', tran_line[9]))
                dest_location = self.env.ref('stock.location_production')
                if not tran_line[16]:
                    tran_line[16] = tran_line[8]
                return self.production_move(tran_line[16], variant_id, location_id, dest_location, tran_line[13], variant_id.uom_id, tran_date)
            
            
            #re-check availability 
            if mrp_production.availability in ('assigned', 'none'):
                mrp_production.action_assign()
                
            #find stock move for this product variant 
            stock_move = mrp_production.move_raw_ids.filtered(lambda q: q.product_id == variant_id and q.state != 'done')
            
            if stock_move.reserved_availability < tran_line[13]:
               
                #unassign other production orders to fulfill this order
                other_moves = self.env['stock.move'].search([("raw_material_production_id", "!=", False), ("product_id", "=", variant_id.id), ("state", "!=", "done")])
                try:
                    other_moves._do_unreserve()
                except Exception as e:
                    pass
                mrp_production.action_assign()
                
            move_lines = stock_move.active_move_line_ids
            
            
            if stock_move.reserved_availability < tran_line[13]:
                if stock_move:
                    missing_qty = tran_line[13] - stock_move.reserved_availability
                    self.inventory_adjust(variant_id, stock_move.location_id, missing_qty, stock_move.product_uom, tran_date)
                mrp_production.action_assign()
                
            move_lines = stock_move.active_move_line_ids
            
            
            if stock_move.active_move_line_ids:
                try: 
                    move_lines[0].qty_done = tran_line[13]
                    stock_move._action_done()
                    move_line_id = stock_move.move_line_ids[0]
                    
                    
                    move_line_id.date = tran_date
                    move_line_id.move_id.date = tran_date
                    x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'stock.move.line', 'res_id':move_line_id.id, 'name':xmlid, 'noupdate':True}) 
                    _logger.info("MRP %s Consume %s DONE!" % (tran_line[16], tran_line[12]))  
                except Exception as e:
                    _logger.error("Error %s while closing [%s]" % (e, tran_line[0]))
                    return False
        
        return True
        
    def import_tran_mfg_produce(self, tran_line):
        is_updated = False
        
        # try:
        tran_date = tran_line[2].strftime("%Y/%m/%d") + tran_line[3].strftime(" %H:%M:%S")
        tran_date = datetime.strptime(tran_date, '%Y/%m/%d %H:%M:%S')
        xmlid = '%s_tran_log_%s' % ("aad", tran_line[0])
        #find item_number in external reference database
        move_line_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
        
        if move_line_id is None:
            mrp_production = self.env.ref('edi_highjump.%s_wo_master_%s' % ('aad', tran_line[16]), raise_if_not_found=False)
            
            if not mrp_production:
                product_tmpl_id = self.env.ref('edi_highjump.%s_item_master_%s' % ("aad", tran_line[12]))
                variant_id = self.env['product.product'].search(['|',('active','=',True),('active','=',False),('product_tmpl_id','=',product_tmpl_id.id),])
            
                #this traveler didnt make it in the import, treat it as an interal move
                dest_location= self.env.ref('edi_highjump.%s_location_%s' % ('aad', tran_line[14].strip()))
                location_id= self.env.ref('stock.location_production')
                if not tran_line[16]:
                    tran_line[16] = tran_line[8]
                return self.production_move(tran_line[16], variant_id, location_id, dest_location, tran_line[13], variant_id.uom_id, tran_date)
            
            stock_move = mrp_production._generate_finished_moves()
            stock_move.quantity_done = tran_line[13]
            stock_move._quantity_done_set()
            move_line_id = stock_move.move_line_ids.filtered(lambda q: q.state != 'done')
            move_line_id.location_dest_id = self.env.ref('edi_highjump.%s_location_%s' % ('aad', tran_line[14].strip()))
            stock_move._action_done()
            
            if mrp_production.qty_produced >= mrp_production.product_qty:
                mrp_production.button_mark_done()
                
            _logger.info("MRP %s Produce %s (%s %s) DONE" % (mrp_production.name, mrp_production.product_id.name, tran_line[13], mrp_production.product_uom_id.name))
            move_line_id = stock_move.move_line_ids[0]
            move_line_id.date = tran_date
            move_line_id.move_id.date = tran_date
            x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'stock.move.line', 'res_id':move_line_id.id, 'name':xmlid, 'noupdate':True}) 
            
            
        return True
        
    def import_tran_internal(self, tran_line):
        is_updated = False
        
        # try:
        tran_date = tran_line[2].strftime("%Y/%m/%d") + tran_line[3].strftime(" %H:%M:%S")
        tran_date = datetime.strptime(tran_date, '%Y/%m/%d %H:%M:%S')
        xmlid = '%s_tran_log_%s' % ("aad", tran_line[0])
        #find item_number in external reference database
        move_line_id = self.env.ref('edi_highjump.%s' % xmlid ,raise_if_not_found=False)
        
        if move_line_id is None:
            product_tmpl_id = self.env.ref('edi_highjump.%s_item_master_%s' % ("aad", tran_line[12]))
            variant_id = self.env['product.product'].search(['|',('active','=',True),('active','=',False),('product_tmpl_id','=',product_tmpl_id.id),])
            location = self.env.ref('edi_highjump.%s_location_%s' % ('aad', tran_line[9].strip()))
            dest_location = self.env.ref('edi_highjump.%s_location_%s' % ('aad', tran_line[14].strip()))
            
            
            new_move = {'name': "int_tlog_%s" % tran_line[0], 'procure_method':'make_to_stock', 'date_expected':tran_line[2], 'date':tran_line[2],'product_id':variant_id.id,'location_dest_id':dest_location.id,'location_id':location.id,'product_uom':variant_id.uom_id.id,'product_uom_qty':tran_line[13],}
            
            if tran_line[13] < 0:
                #reverse this tran line
                new_move['location_dest_id'] = location.id
                new_move['location_id'] = dest_location.id
                new_move['product_uom_qty'] = abs(new_move['product_uom_qty'])
                
            stock_move = self.env['stock.move'].create(new_move)
            stock_move._action_confirm()
            stock_move._action_assign()
            
            if stock_move.reserved_availability < tran_line[13]:
                #location inventory is less than the moves required quantity, add an adjustment to the location.
                location_qty = self.env['stock.quant']._get_available_quantity(stock_move.product_id, stock_move.location_id, allow_negative=True)
                missing_qty = tran_line[13] - location_qty
                
                self.inventory_adjust(variant_id, stock_move.location_id, abs(missing_qty), stock_move.product_uom, tran_date)
                stock_move._action_assign()
                
            if not stock_move.reserved_availability >= tran_line[13]:
                #something still went wrong, abort this move.
                _logger.error("Assignnig [%s of %s] %s in location %s" % (stock_move.reserved_availability, stock_move.product_uom_qty, stock_move.product_id.default_code, tran_line[9].strip(),))
                stock_move._action_cancel()
                stock_move.unlink()
                return False
            
            try:
                #finsih this move
                stock_move.move_line_ids[0].qty_done = tran_line[13]
                stock_move._action_done()
                _logger.info("Interal move %s DONE!" % (tran_line[12]))  
            except Exception as e:
                _logger.error("Error %s while closing [%s]" % (e, tran_line[0]))
                return False
            
            #adjust the date done back to origional date from t_log
            move_line_id = stock_move.move_line_ids[0]
            move_line_id.date = tran_date
            stock_move.date = tran_date
            x_ref = self.env['ir.model.data'].create({'module':'edi_highjump', 'model':'stock.move.line', 'res_id':move_line_id.id, 'name':xmlid, 'noupdate':True})
                
        # except Exception as e:
        #     _logger.error("Error %s while importing [%s]" % (e, tran_line[0]))
        #     return False
            
        return True
    
    def inventory_adjust(self, product_id, location_dest_id, uom_qty, uom_id, date):
        _logger.warn("OTF Adjustment %s DONE!" % (product_id.default_code)  )
        return self.inventory_move('ADJ_%s' % product_id.default_code, product_id, self.env.ref('stock.location_inventory'), location_dest_id, uom_qty, uom_id, date)
    
    def production_move(self, name, product_id, location_id, location_dest_id, uom_qty, uom_id, date):
        uom_qty=abs(uom_qty)
        _logger.info("OTF-MRP %s - %s DONE!" % (name, product_id.default_code)  )
        return self.inventory_move('MRP_%s' % (name), product_id, location_id, location_dest_id, uom_qty, uom_id, date)
        
    def inventory_move(self, name, product_id, location_id, location_dest_id, uom_qty, uom_id, date):
        new_move = {    'name': name, 
                        'procure_method':'make_to_stock', 
                        'date_expected':datetime.now(), 
                        'date':datetime.now(),
                        'product_id':product_id.id,
                        'location_dest_id':location_dest_id.id,
                        'location_id':location_id.id,
                        'product_uom':uom_id.id,
                        'product_uom_qty':uom_qty,
            
                    }
        adjustment_move = self.env['stock.move'].create(new_move)
        adjustment_move._action_confirm()
        adjustment_move._action_assign()
        if adjustment_move.move_line_ids:
            
            move_lines = adjustment_move.move_line_ids
            adjustment_move.move_line_ids[0].date = datetime.now()
            adjustment_move.move_line_ids[0].qty_done = uom_qty
            adjustment_move._action_done()
            
            #adjust data back to orgional date
            try:
                adjustment_move.move_line_ids[0].date = date
            except Exception as e:
                pass
            adjustment_move.date = date
            
       
        return adjustment_move
        
    def get_trav_num(self, connection, outsideID):
        sql = ""
        sql += "SELECT work_order_number FROM t_prod_receipt  WHERE receipt_id='%s';" % (outsideID)
        
        tran_lines =  connection.execute(sql).fetchall()
        if tran_lines:
            return tran_lines[0][0]
        
        return None
        