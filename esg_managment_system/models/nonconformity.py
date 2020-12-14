from odoo import models, api, fields, _

class MgmtsystemNonconformity(models.Model):

    _inherit = "mgmtsystem.nonconformity"
    
    buyer_id = fields.Many2one('res.partner', 'Buyer')
    product_ids = fields.Many2many(comodel_name='product.product')
    due_date = fields.Datetime('Due Date', required=True)
    severity = fields.Selection([('1', 'Level 1'),('2', 'Level 2'),('3', 'Level 3')],'NCMR Level', track_visibility='onchange', required=True,)
    description = fields.Html('Description', required=True)