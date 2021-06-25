from odoo import api, fields, models, http
from odoo.http import request
from odoo.addons.portal.controllers.portal import CustomerPortal
import pyodbc 
import logging, time, json
from datetime import datetime, timedelta
_logger = logging.getLogger(__name__)

class edi_esg_ats_import(models.Model):
    _name = "edi.esg_ats"
    _description = "EDI ESG ATS"
        
    def import_ats(self):
        #create SQL Server database object.
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=eccosqltest01;Database=ATSDATA;UID=reports;PWD=esi')
        cursor = conn.cursor()
        
        self. import_tc_calibration(cursor)
        #close up SQL connections and cursors.
        cursor.close()
        conn.close()   
        
        return True
        
    def import_tc_calibration(self, cursor):
        config_last_date = self.env.ref('edi_esg_ats.dailycal_last_date')
        last_date = datetime.strptime(config_last_date.value, '%Y-%m-%d %H:%M:%S')
        
        sql = ""
        sql += "SELECT TOP 1000 [ATS],[CalDateTime],[Model],[VoltsPerPascal],[comment],[UserName],[Stage],[CalibratorId]"
        sql += " FROM [ATSDATA].[dbo].[DailyCalCheck]"
        sql += " WHERE VoltsPerPascal > 10 AND (Model = '' OR Model is null) and CalDateTime > '%s' " % last_date
        sql += " order by CalDateTime ASC"
        
        for item in cursor.execute(sql).fetchall():
            try:
                calibration_vals = {}
                calibration_vals['equipment_id'] = self.env.ref('edi_esg_ats.ATS_%s' % (item.ATS)).id
                calibration_vals['procedure_id'] = self.env.ref('edi_esg_ats.calibration_procedure').id
                calibration_vals['calibration_start_date'] =  item.CalDateTime 
                calibration_vals['calibration_end_date'] =  calibration_vals['calibration_start_date'] + timedelta(hours=6)
                
                calibration = self.env['maintenance.calibration'].create(calibration_vals)
                calibration.on_change_procedure_id()
                
                calibration.test_ids[0].initial_value = item.Stage or item.VoltsPerPascal
                calibration.test_ids[0].final_value = item.VoltsPerPascal
                
                #search for maintenance.equipment used as test tool
                equipment_id = self.env['maintenance.equipment'].search(["|",("serial_no","=", item.CalibratorId),("code","=", item.CalibratorId)])
                if len(equipment_id):
                    calibration.test_ids[0].test_equipment_id = equipment_id[0].id
                
                last_date = item.CalDateTime 
                _logger.info("Added TC Calibration for TC%s" % (item.ATS))
            except Exception as e:
                pass
           
            
        config_last_date.value =  last_date.strftime('%Y-%m-%d %H:%M:%S')

class CustomerPortal(CustomerPortal):
    @http.route(['/maintenance/ats/<string:Equipment_ID>'], type='http', auth="public", website=False)
    def maintenance_eqipment(self, Equipment_ID, **kw):
        equipment_id = request.env['maintenance.equipment'].search(["|",("serial_no","=", Equipment_ID),("code","=", Equipment_ID)])
        
        if not len(equipment_id):
            return '{"0": "No Result Found"}'
            
        result = {}
        result[equipment_id.id] = self.build_equipment_dict(equipment_id)
        
        return json.dumps(result, indent=4)
        
    def build_equipment_dict(self, equipment_id):
        result = {}
        result['id'] = equipment_id.id
        result['name'] = equipment_id.name
        result['status'] = {'id': equipment_id.status_id.id, 'name': equipment_id.status_id.name}
        result['calibration_type'] = {'name':equipment_id.calibration_type.name, 'id':equipment_id.calibration_type.id}
        result['calibration_status'] = equipment_id.calibration_status
        result['calibrated'] = equipment_id.calibrated
        result['code'] = equipment_id.code
        result['serial_no'] = equipment_id.serial_no
        result['model'] = equipment_id.model
        
        children = {}
        for child in equipment_id.child_ids:
            children[child.id] = self.build_equipment_dict(child)
        result['children'] = children
        
        return result