from odoo import api, fields, models


class MrpRoutingWorkcenter(models.Model):
    _inherit = 'mrp.routing.workcenter'
    
    quality_test = fields.Many2one('qc.test', 'Quality Control')
    
    
class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'
    
    quality_test = fields.Many2one('qc.test', 'Quality Control')
    
class QcInspection(models.Model):
    _inherit = 'qc.inspection'
    
    mrp_workorder_id = fields.Many2one('mrp.workorder', 'MRP Workorder')
    
    
    def prepare_inspection_lines(self):
        self.inspection_lines = self._prepare_inspection_lines(self.test)
        return True
        
class MrpProduction(models.Model):
    _inherit = 'mrp.production'
    
    def _workorders_create(self, bom, bom_data):
        workorders = super(MrpProduction, self)._workorders_create(bom, bom_data)
        
        for workorder in workorders:
            workorder.quality_test = workorder.operation_id.quality_test
            
        return workorders